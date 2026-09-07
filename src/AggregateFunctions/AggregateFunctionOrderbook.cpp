#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

#include <Functions/FunctionHelpers.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/assert_cast.h>

#include <algorithm>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Depth-row op (parallel to prices/sizes). Matches Enum('insert','change','cancel')
/// (CH Enum defaults start at 1 — same style as kind Enum('snapshot','delta')).
enum class DepthOp : UInt8
{
    Insert = 1,
    Change = 2,
    Cancel = 3,
};

/// One orderbook level. Price / sizes as Decimal128(19) bits (same as MarketLens).
/// Sizes are signed: bid > 0, ask < 0 (no separate Bool sign).
struct OrderbookLevel
{
    Int128 price = 0;
    DateTime64 ts{0}; // scale 6
    Int128 base = 0;  // signed Decimal128(19) bits
    Int128 quote = 0; // signed Decimal128(19) bits
    /// If true, a final zero is a tombstone (level may have lived before this state).
    /// Set by: first-sighting change/cancel (`from_new`).
    /// Cleared by: first-sighting insert or snapshot membership (`from_new` / snapshot).
    /// Not cleared by insert when updating an existing state level (cancel→insert must
    /// keep the prior witness so a later cancel still tombstones).
    bool existed_before = false;

    bool isZero() const { return base == 0 && quote == 0; }
};

struct AggregateFunctionOrderbookData
{
    /// v3: sizes are signed Tuple(base, quote); dropped Bool sign.
    static constexpr UInt8 kSerializationVersion = 3;

    DateTime64 snapshot_ts{0};
    /// Strictly sorted by price; at most one entry per price.
    std::vector<OrderbookLevel> levels;

    void pruneBefore(DateTime64 cutoff)
    {
        if (cutoff.value == 0)
            return;

        levels.erase(
            std::remove_if(
                levels.begin(),
                levels.end(),
                [&](const OrderbookLevel & lvl) { return lvl.ts < cutoff; }),
            levels.end());
    }
};

/// orderbook(prices, sizes, ops, ts, kind)
///
/// prices: Array(Decimal128(19)) | Array(Int128) — strictly ascending, no duplicates
/// sizes:  Array(Tuple(Decimal128(19), Decimal128(19))) — signed (base, quote); bid>0 ask<0
/// ops:    Array(Enum('insert', 'change', 'cancel')) — values 1, 2, 3
/// ts:     DateTime64(6)
/// kind:   Enum('snapshot', 'delta') — values 1, 2 (same as raw orderbooks / Rust OrderbookKind)
///
/// Zero drop:
/// - If state has snapshot_ts: drop all zeros (bucket applies as a snapshot; absence = gone).
/// - Else: drop zeros only when !existed_before (ephemeral insert…cancel).
/// First sighting: insert/snapshot → eb=false; change/cancel → eb=true.
/// Updates preserve prev.eb (including insert after cancel — do not clear the witness).
/// Snapshot row-kind forces eb=false on apply.
/// State merge: size/ts from later; eb from earlier, including rebirth (earlier 0, later +)
/// so a cancel tombstone is not lost when merged with a later insert.
class AggregateFunctionOrderbook final
    : public IAggregateFunctionDataHelper<AggregateFunctionOrderbookData, AggregateFunctionOrderbook>
{
private:
    DataTypePtr price_type;
    DataTypePtr datetime_type;
    bool prices_are_decimal = true;

    static DataTypePtr makeSizeTupleType()
    {
        return std::make_shared<DataTypeTuple>(DataTypes{
            std::make_shared<DataTypeDecimal<Decimal128>>(38, 19),
            std::make_shared<DataTypeDecimal<Decimal128>>(38, 19),
        });
    }

    static DataTypePtr makeLevelTupleType(const DataTypePtr & price_ty, const DataTypePtr & dt_ty)
    {
        return std::make_shared<DataTypeTuple>(DataTypes{
            price_ty,
            dt_ty,
            makeSizeTupleType(),
        });
    }

public:
    AggregateFunctionOrderbook(
        const DataTypes & argument_types_,
        const Array & parameters_,
        DataTypePtr price_type_,
        DataTypePtr datetime_type_,
        bool prices_are_decimal_)
        : IAggregateFunctionDataHelper(
              argument_types_,
              parameters_,
              std::make_shared<DataTypeTuple>(DataTypes{
                  datetime_type_,
                  std::make_shared<DataTypeArray>(makeLevelTupleType(price_type_, datetime_type_)),
              }))
        , price_type(std::move(price_type_))
        , datetime_type(std::move(datetime_type_))
        , prices_are_decimal(prices_are_decimal_)
    {
    }

    String getName() const override { return "orderbook"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & data = this->data(place);

        const auto & prices_arr = assert_cast<const ColumnArray &>(*columns[0]);
        const auto & sizes_arr = assert_cast<const ColumnArray &>(*columns[1]);
        const auto & ops_arr = assert_cast<const ColumnArray &>(*columns[2]);

        const auto & prices_offsets = prices_arr.getOffsets();
        const auto & sizes_offsets = sizes_arr.getOffsets();
        const auto & ops_offsets = ops_arr.getOffsets();
        const size_t prices_offset = prices_offsets[row_num - 1];
        const size_t prices_len = prices_arr.getSize(row_num);
        const size_t sizes_offset = sizes_offsets[row_num - 1];
        const size_t sizes_len = sizes_arr.getSize(row_num);
        const size_t ops_offset = ops_offsets[row_num - 1];
        const size_t ops_len = ops_arr.getSize(row_num);

        if (prices_len != sizes_len || prices_len != ops_len)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "orderbook: prices, sizes, and ops arrays must have equal length (got {}, {}, {})",
                prices_len,
                sizes_len,
                ops_len);

        const auto event_ts = assert_cast<const ColumnDateTime64 &>(*columns[3]).getData()[row_num];
        // Enum('snapshot','delta') → snapshot=1, delta=2 (CH Enum defaults start at 1).
        const auto row_kind = assert_cast<const ColumnInt8 &>(*columns[4]).getData()[row_num];
        const bool is_snapshot = row_kind == 1;

        if (is_snapshot && data.snapshot_ts < event_ts)
        {
            data.snapshot_ts = event_ts;
            data.pruneBefore(event_ts);
        }

        if (prices_len == 0)
            return;

        std::vector<BatchLevel> batch;
        batch.reserve(prices_len);

        const auto & sizes_tuple = assert_cast<const ColumnTuple &>(sizes_arr.getData());
        const auto & base_col = assert_cast<const ColumnDecimal<Decimal128> &>(sizes_tuple.getColumn(0)).getData();
        const auto & quote_col = assert_cast<const ColumnDecimal<Decimal128> &>(sizes_tuple.getColumn(1)).getData();
        const auto & ops_col = assert_cast<const ColumnInt8 &>(ops_arr.getData()).getData();

        for (size_t i = 0; i < prices_len; ++i)
        {
            BatchLevel item;
            item.lvl.ts = event_ts;
            item.lvl.base = base_col[sizes_offset + i].value;
            item.lvl.quote = quote_col[sizes_offset + i].value;

            if (prices_are_decimal)
            {
                const auto & price_col
                    = assert_cast<const ColumnDecimal<Decimal128> &>(prices_arr.getData());
                item.lvl.price = price_col.getData()[prices_offset + i].value;
            }
            else
            {
                const auto & price_col = assert_cast<const ColumnVector<Int128> &>(prices_arr.getData());
                item.lvl.price = price_col.getData()[prices_offset + i];
            }

            const Int8 op_raw = ops_col[ops_offset + i];
            if (op_raw < static_cast<Int8>(DepthOp::Insert)
                || op_raw > static_cast<Int8>(DepthOp::Cancel))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "orderbook: invalid depth op {}", Int32(op_raw));
            item.op = static_cast<DepthOp>(op_raw);

            if (item.op == DepthOp::Cancel && !item.lvl.isZero())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "orderbook: cancel op requires zero size");

            if (is_snapshot && item.lvl.isZero())
                continue; // absent from snap ⇒ gone after prune

            batch.push_back(item);
        }

        data.levels = applyBatch(data.levels, batch, data.snapshot_ts, is_snapshot);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs_place, Arena *) const override
    {
        auto & lhs = this->data(place);
        const auto & rhs = this->data(rhs_place);

        if (lhs.snapshot_ts < rhs.snapshot_ts)
            lhs.snapshot_ts = rhs.snapshot_ts;

        lhs.levels = mergeLevels(lhs.levels, rhs.levels, lhs.snapshot_ts);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & data = this->data(place);
        writeIntBinary(AggregateFunctionOrderbookData::kSerializationVersion, buf);
        writeIntBinary(data.snapshot_ts.value, buf);
        writeVarUInt(data.levels.size(), buf);
        for (const auto & lvl : data.levels)
        {
            writeIntBinary(lvl.price, buf);
            writeIntBinary(lvl.ts.value, buf);
            writeIntBinary(lvl.base, buf);
            writeIntBinary(lvl.quote, buf);
            writeIntBinary(static_cast<UInt8>(lvl.existed_before), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & data = this->data(place);
        UInt8 version = 0;
        readIntBinary(version, buf);
        if (version != AggregateFunctionOrderbookData::kSerializationVersion)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "orderbook: unsupported state version {}", UInt32(version));

        Int64 snap = 0;
        readIntBinary(snap, buf);
        data.snapshot_ts = DateTime64(snap);

        size_t n = 0;
        readVarUInt(n, buf);
        data.levels.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            auto & lvl = data.levels[i];
            Int64 ts = 0;
            UInt8 existed = 0;
            readIntBinary(lvl.price, buf);
            readIntBinary(ts, buf);
            lvl.ts = DateTime64(ts);
            readIntBinary(lvl.base, buf);
            readIntBinary(lvl.quote, buf);
            readIntBinary(existed, buf);
            lvl.existed_before = existed != 0;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        auto & to_tuple = assert_cast<ColumnTuple &>(to);

        auto & snap_col = assert_cast<ColumnDateTime64 &>(to_tuple.getColumn(0));
        snap_col.getData().push_back(data.snapshot_ts);

        auto & levels_arr = assert_cast<ColumnArray &>(to_tuple.getColumn(1));
        auto & levels_tuple = assert_cast<ColumnTuple &>(levels_arr.getData());

        auto & out_prices = levels_tuple.getColumn(0);
        auto & out_tss = assert_cast<ColumnDateTime64 &>(levels_tuple.getColumn(1));
        auto & out_sizes = assert_cast<ColumnTuple &>(levels_tuple.getColumn(2));
        auto & out_base = assert_cast<ColumnDecimal<Decimal128> &>(out_sizes.getColumn(0));
        auto & out_quote = assert_cast<ColumnDecimal<Decimal128> &>(out_sizes.getColumn(1));

        const size_t n = data.levels.size();
        levels_arr.getOffsets().push_back(levels_arr.getOffsets().back() + n);

        for (const auto & lvl : data.levels)
        {
            if (prices_are_decimal)
                assert_cast<ColumnDecimal<Decimal128> &>(out_prices).insertValue(Decimal128(lvl.price));
            else
                assert_cast<ColumnVector<Int128> &>(out_prices).insertValue(lvl.price);

            out_tss.insertValue(lvl.ts);
            out_base.insertValue(Decimal128(lvl.base));
            out_quote.insertValue(Decimal128(lvl.quote));
        }
    }

private:
    struct BatchLevel
    {
        OrderbookLevel lvl;
        DepthOp op = DepthOp::Insert;
    };

    static bool shouldKeep(const OrderbookLevel & lvl, DateTime64 snapshot_ts)
    {
        if (snapshot_ts.value != 0 && lvl.ts < snapshot_ts)
            return false;
        // State with a snapshot is applied as a full book replace: no zero tombstones.
        if (lvl.isZero() && snapshot_ts.value != 0)
            return false;
        if (lvl.isZero() && !lvl.existed_before)
            return false;
        return true;
    }

    /// Apply a sorted batch of depth ops onto sorted state.
    static std::vector<OrderbookLevel> applyBatch(
        const std::vector<OrderbookLevel> & state,
        const std::vector<BatchLevel> & batch,
        DateTime64 snapshot_ts,
        bool is_snapshot)
    {
        std::vector<OrderbookLevel> out;
        out.reserve(state.size() + batch.size());

        size_t i = 0;
        size_t j = 0;

        const auto emit = [&](const OrderbookLevel & lvl) {
            if (shouldKeep(lvl, snapshot_ts))
                out.push_back(lvl);
        };

        const auto from_new = [&](const BatchLevel & item) -> OrderbookLevel {
            OrderbookLevel lvl = item.lvl;
            if (is_snapshot || item.op == DepthOp::Insert)
                // Snapshot = replace after prune; insert = born in this state.
                lvl.existed_before = false;
            else // change or cancel on unseen price (delta-only prior life)
                lvl.existed_before = true;
            return lvl;
        };

        const auto from_update = [&](const OrderbookLevel & prev, const BatchLevel & item) -> OrderbookLevel {
            if (item.lvl.ts < prev.ts)
                return prev;

            OrderbookLevel lvl = item.lvl;
            if (is_snapshot)
                lvl.existed_before = false; // snapshot replace: clean slate
            else
                // Preserve witness of life before this state (cancel→insert→cancel must
                // still tombstone; insert alone must not clear a prior cancel's eb).
                lvl.existed_before = prev.existed_before;
            return lvl;
        };

        while (i < state.size() && j < batch.size())
        {
            if (state[i].price < batch[j].lvl.price)
                emit(state[i++]);
            else if (state[i].price > batch[j].lvl.price)
                emit(from_new(batch[j++]));
            else
            {
                emit(from_update(state[i], batch[j]));
                ++i;
                ++j;
            }
        }
        while (i < state.size())
            emit(state[i++]);
        while (j < batch.size())
            emit(from_new(batch[j++]));

        return out;
    }

    /// Merge two states (no ops). Size/ts LWW (later wins); existed_before from earlier
    /// (earlier witness of "lived before this state"), including rebirth
    /// (earlier zero tombstone + later live insert must keep earlier.eb).
    static std::vector<OrderbookLevel> mergeLevels(
        const std::vector<OrderbookLevel> & a,
        const std::vector<OrderbookLevel> & b,
        DateTime64 snapshot_ts)
    {
        std::vector<OrderbookLevel> out;
        out.reserve(a.size() + b.size());

        size_t i = 0;
        size_t j = 0;

        const auto emit = [&](OrderbookLevel lvl) {
            if (shouldKeep(lvl, snapshot_ts))
                out.push_back(lvl);
        };

        const auto combine = [](const OrderbookLevel & x, const OrderbookLevel & y) {
            const OrderbookLevel & earlier = (x.ts <= y.ts) ? x : y;
            const OrderbookLevel & later = (x.ts <= y.ts) ? y : x;
            OrderbookLevel w = later; // size/ts from later (tie → y when x.ts == y.ts via <=)
            w.existed_before = earlier.existed_before;
            return w;
        };

        while (i < a.size() && j < b.size())
        {
            if (a[i].price < b[j].price)
                emit(a[i++]);
            else if (a[i].price > b[j].price)
                emit(b[j++]);
            else
            {
                emit(combine(a[i], b[j]));
                ++i;
                ++j;
            }
        }
        while (i < a.size())
            emit(a[i++]);
        while (j < b.size())
            emit(b[j++]);

        return out;
    }
};

AggregateFunctionPtr createAggregateFunctionOrderbook(
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings *)
{
    assertNoParameters(name, parameters);

    if (argument_types.size() != 5)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires 5 arguments: "
            "Array(price), Array(Tuple(Decimal128(19), Decimal128(19))), "
            "Array(Enum), DateTime64(6), Enum",
            name);

    const auto * prices_array = checkAndGetDataType<DataTypeArray>(argument_types[0].get());
    if (!prices_array)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 1 for function {} must be Array, got {}",
            name,
            argument_types[0]->getName());

    bool prices_are_decimal = false;
    DataTypePtr price_type = prices_array->getNestedType();
    if (const auto * dec = checkDecimal<Decimal128>(*price_type))
    {
        prices_are_decimal = true;
        if (dec->getScale() != 19)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument 1 nested type for function {} must be Decimal128(19) or Int128, got {}",
                name,
                price_type->getName());
    }
    else if (!isInt128(price_type))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 1 nested type for function {} must be Decimal128(19) or Int128, got {}",
            name,
            price_type->getName());
    }

    const auto * sizes_array = checkAndGetDataType<DataTypeArray>(argument_types[1].get());
    if (!sizes_array)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 2 for function {} must be Array, got {}",
            name,
            argument_types[1]->getName());

    const auto * size_tuple = checkAndGetDataType<DataTypeTuple>(sizes_array->getNestedType().get());
    if (!size_tuple || size_tuple->getElements().size() != 2)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 2 nested type for function {} must be Tuple(Decimal128(19), Decimal128(19)), got {}",
            name,
            sizes_array->getNestedType()->getName());

    for (size_t k : {size_t(0), size_t(1)})
    {
        const auto * dec = checkDecimal<Decimal128>(*size_tuple->getElements()[k]);
        if (!dec || dec->getScale() != 19)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "sizes.{} for function {} must be Decimal128(19), got {}",
                k + 1,
                name,
                size_tuple->getElements()[k]->getName());
    }

    const auto * ops_array = checkAndGetDataType<DataTypeArray>(argument_types[2].get());
    if (!ops_array)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 3 for function {} must be Array, got {}",
            name,
            argument_types[2]->getName());

    const auto * ops_enum = checkAndGetDataType<DataTypeEnum8>(ops_array->getNestedType().get());
    if (!ops_enum)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 3 nested type for function {} must be Enum('insert','change','cancel'), got {}",
            name,
            ops_array->getNestedType()->getName());

    const auto * dt = checkAndGetDataType<DataTypeDateTime64>(argument_types[3].get());
    if (!dt || dt->getScale() != 6)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 4 for function {} must be DateTime64(6), got {}",
            name,
            argument_types[3]->getName());

    const auto * kind = checkAndGetDataType<DataTypeEnum8>(argument_types[4].get());
    if (!kind)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 5 for function {} must be Enum8, got {}",
            name,
            argument_types[4]->getName());

    return std::make_shared<AggregateFunctionOrderbook>(
        argument_types, parameters, price_type, argument_types[3], prices_are_decimal);
}

}

void registerAggregateFunctionOrderbook(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Merges orderbook depth updates into a sorted book.
Per-level ops: insert / change / cancel. Row-level kind: Enum('snapshot', 'delta').
Last-write-wins by timestamp; snapshots prune older levels.
If the state has a snapshot_ts, final zeros are dropped (applied as a snapshot).
Otherwise zeros are kept only as tombstones when existed_before
(first-sighting change/cancel, preserved across later ops including insert);
a pure insert…cancel with no prior witness is ephemeral and dropped.
Incoming prices must be strictly ascending with no duplicates.
    )";
    FunctionDocumentation::Syntax syntax = "orderbook(prices, sizes, ops, ts, kind)";
    FunctionDocumentation::Arguments arguments = {
        {"prices", "Sorted unique price levels.", {"Array(Decimal128(19))", "Array(Int128)"}},
        {"sizes", "Parallel signed (base, quote) sizes; bid>0 ask<0.", {"Array(Tuple(Decimal128(19), Decimal128(19)))"}},
        {"ops", "Per-level insert/change/cancel.", {"Array(Enum)"}},
        {"ts", "Event timestamp.", {"DateTime64(6)"}},
        {"kind", "Row-level snapshot or delta.", {"Enum"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Tuple(snapshot_ts, Array(Tuple(price, level_ts, Tuple(base, quote)))).",
        {"Tuple"},
    };
    FunctionDocumentation::Examples examples;
    FunctionDocumentation::IntroducedIn introduced_in = {26, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation
        = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    AggregateFunctionProperties properties
        = {.returns_default_when_only_null = true, .is_order_dependent = true};
    factory.registerFunction("orderbook", {createAggregateFunctionOrderbook, documentation, properties});
}

}
