#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>

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

/// One orderbook level. Price stored as Decimal128(19) bits (same as Map(Int128, …) keys in MarketLens).
struct OrderbookLevel
{
    Int128 price = 0;
    DateTime64 ts{0}; // scale 6
    UInt8 sign = 0;   // TripleSize.0
    Int128 base = 0;  // TripleSize.1 Decimal128(19) bits
    Int128 quote = 0; // TripleSize.2 Decimal128(19) bits

    bool isZero() const { return base == 0 && quote == 0; }
};

struct AggregateFunctionMergeOrderbookData
{
    static constexpr UInt8 kSerializationVersion = 1;

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

/// mergeOrderbook(prices, sizes, ts, kind)
///
/// prices: Array(Decimal128(19))  — or Array(Int128) accepted as price bits
/// sizes:  Array(Tuple(Bool, Decimal128(19), Decimal128(19)))
/// ts:     DateTime64(6)
/// kind:   Enum8('delta' = 0, 'snapshot' = 1)
///
/// Incoming prices must be strictly ascending with no duplicates (recorder contract).
/// State: sorted levels + snapshot_ts. Merge is linear LWW-by-ts; snapshots prune
/// older levels; zero sizes are dropped (no cemetery).
class AggregateFunctionMergeOrderbook final
    : public IAggregateFunctionDataHelper<AggregateFunctionMergeOrderbookData, AggregateFunctionMergeOrderbook>
{
private:
    DataTypePtr price_type;
    DataTypePtr size_triple_type;
    DataTypePtr datetime_type;
    bool prices_are_decimal = true;

    static DataTypePtr makeSizeTripleType()
    {
        return std::make_shared<DataTypeTuple>(DataTypes{
            std::make_shared<DataTypeUInt8>(), // Bool stored as UInt8
            std::make_shared<DataTypeDecimal<Decimal128>>(38, 19),
            std::make_shared<DataTypeDecimal<Decimal128>>(38, 19),
        });
    }

    static DataTypePtr makeLevelTupleType(const DataTypePtr & price_ty, const DataTypePtr & dt_ty)
    {
        return std::make_shared<DataTypeTuple>(DataTypes{
            price_ty,
            dt_ty,
            makeSizeTripleType(),
        });
    }

public:
    AggregateFunctionMergeOrderbook(
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
        , size_triple_type(makeSizeTripleType())
        , datetime_type(std::move(datetime_type_))
        , prices_are_decimal(prices_are_decimal_)
    {
    }

    String getName() const override { return "mergeOrderbook"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & data = this->data(place);

        const auto & prices_arr = assert_cast<const ColumnArray &>(*columns[0]);
        const auto & sizes_arr = assert_cast<const ColumnArray &>(*columns[1]);

        const size_t prices_offset = prices_arr.offsetAt(row_num);
        const size_t prices_len = prices_arr.sizeAt(row_num);
        const size_t sizes_offset = sizes_arr.offsetAt(row_num);
        const size_t sizes_len = sizes_arr.sizeAt(row_num);

        if (prices_len != sizes_len)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "mergeOrderbook: prices and sizes arrays must have equal length (got {} and {})",
                prices_len,
                sizes_len);

        const auto event_ts = assert_cast<const ColumnDateTime64 &>(*columns[2]).getData()[row_num];
        const auto kind = assert_cast<const ColumnInt8 &>(*columns[3]).getData()[row_num]; // 0=delta, 1=snapshot

        if (kind == 1 && data.snapshot_ts < event_ts)
        {
            data.snapshot_ts = event_ts;
            data.pruneBefore(event_ts);
        }

        if (prices_len == 0)
            return;

        std::vector<OrderbookLevel> batch;
        batch.reserve(prices_len);

        const auto & sizes_tuple = assert_cast<const ColumnTuple &>(sizes_arr.getData());
        const auto & sign_col = assert_cast<const ColumnUInt8 &>(sizes_tuple.getColumn(0)).getData();
        const auto & base_col = assert_cast<const ColumnDecimal<Decimal128> &>(sizes_tuple.getColumn(1)).getData();
        const auto & quote_col = assert_cast<const ColumnDecimal<Decimal128> &>(sizes_tuple.getColumn(2)).getData();

        // Caller guarantees prices are strictly ascending with no duplicates.
        for (size_t i = 0; i < prices_len; ++i)
        {
            OrderbookLevel lvl;
            lvl.ts = event_ts;
            lvl.sign = sign_col[sizes_offset + i];
            lvl.base = base_col[sizes_offset + i].value;
            lvl.quote = quote_col[sizes_offset + i].value;

            if (prices_are_decimal)
            {
                const auto & price_col
                    = assert_cast<const ColumnDecimal<Decimal128> &>(prices_arr.getData());
                lvl.price = price_col.getData()[prices_offset + i].value;
            }
            else
            {
                const auto & price_col = assert_cast<const ColumnVector<Int128> &>(prices_arr.getData());
                lvl.price = price_col.getData()[prices_offset + i];
            }

            batch.push_back(lvl);
        }

        data.levels = mergeLevels(data.levels, batch, data.snapshot_ts);
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
        writeIntBinary(AggregateFunctionMergeOrderbookData::kSerializationVersion, buf);
        writeIntBinary(data.snapshot_ts.value, buf);
        writeVarUInt(data.levels.size(), buf);
        for (const auto & lvl : data.levels)
        {
            writeIntBinary(lvl.price, buf);
            writeIntBinary(lvl.ts.value, buf);
            writeIntBinary(lvl.sign, buf);
            writeIntBinary(lvl.base, buf);
            writeIntBinary(lvl.quote, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & data = this->data(place);
        UInt8 version = 0;
        readIntBinary(version, buf);
        if (version != AggregateFunctionMergeOrderbookData::kSerializationVersion)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "mergeOrderbook: unsupported state version {}", version);

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
            readIntBinary(lvl.price, buf);
            readIntBinary(ts, buf);
            lvl.ts = DateTime64(ts);
            readIntBinary(lvl.sign, buf);
            readIntBinary(lvl.base, buf);
            readIntBinary(lvl.quote, buf);
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
        auto & out_sign = assert_cast<ColumnUInt8 &>(out_sizes.getColumn(0));
        auto & out_base = assert_cast<ColumnDecimal<Decimal128> &>(out_sizes.getColumn(1));
        auto & out_quote = assert_cast<ColumnDecimal<Decimal128> &>(out_sizes.getColumn(2));

        const size_t n = data.levels.size();
        levels_arr.getOffsets().push_back(levels_arr.getOffsets().back() + n);

        for (const auto & lvl : data.levels)
        {
            if (prices_are_decimal)
                assert_cast<ColumnDecimal<Decimal128> &>(out_prices).insertValue(Decimal128(lvl.price));
            else
                assert_cast<ColumnVector<Int128> &>(out_prices).insertValue(lvl.price);

            out_tss.insertValue(lvl.ts);
            out_sign.insertValue(lvl.sign);
            out_base.insertValue(Decimal128(lvl.base));
            out_quote.insertValue(Decimal128(lvl.quote));
        }
    }

private:
    /// Linear merge of two price-sorted level lists. LWW by ts; drop zeros; drop ts < snapshot_ts.
    static std::vector<OrderbookLevel> mergeLevels(
        const std::vector<OrderbookLevel> & a,
        const std::vector<OrderbookLevel> & b,
        DateTime64 snapshot_ts)
    {
        std::vector<OrderbookLevel> out;
        out.reserve(a.size() + b.size());

        size_t i = 0;
        size_t j = 0;
        const auto emit = [&](const OrderbookLevel & lvl) {
            if (snapshot_ts.value != 0 && lvl.ts < snapshot_ts)
                return;
            if (lvl.isZero())
                return;
            out.push_back(lvl);
        };

        while (i < a.size() && j < b.size())
        {
            if (a[i].price < b[j].price)
            {
                emit(a[i++]);
            }
            else if (a[i].price > b[j].price)
            {
                emit(b[j++]);
            }
            else
            {
                // Same price: higher ts wins; tie → prefer b (rhs).
                if (a[i].ts > b[j].ts)
                    emit(a[i]);
                else
                    emit(b[j]);
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

AggregateFunctionPtr createAggregateFunctionMergeOrderbook(
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings *)
{
    assertNoParameters(name, parameters);

    if (argument_types.size() != 4)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires 4 arguments: "
            "Array(price), Array(Tuple(Bool, Decimal128(19), Decimal128(19))), DateTime64(6), Enum8",
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
    if (!size_tuple || size_tuple->getElements().size() != 3)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 2 nested type for function {} must be Tuple(Bool/UInt8, Decimal128(19), Decimal128(19)), got {}",
            name,
            sizes_array->getNestedType()->getName());

    // sign: Bool or UInt8
    const auto & sign_ty = size_tuple->getElements()[0];
    if (!(isUInt8(sign_ty) || isBool(sign_ty)))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "sizes.1 for function {} must be Bool or UInt8, got {}",
            name,
            sign_ty->getName());

    for (size_t k : {size_t(1), size_t(2)})
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

    const auto * dt = checkAndGetDataType<DataTypeDateTime64>(argument_types[2].get());
    if (!dt || dt->getScale() != 6)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 3 for function {} must be DateTime64(6), got {}",
            name,
            argument_types[2]->getName());

    const auto * kind = checkAndGetDataType<DataTypeEnum8>(argument_types[3].get());
    if (!kind)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument 4 for function {} must be Enum8, got {}",
            name,
            argument_types[3]->getName());

    return std::make_shared<AggregateFunctionMergeOrderbook>(
        argument_types, parameters, price_type, argument_types[2], prices_are_decimal);
}

}

void registerAggregateFunctionMergeOrderbook(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Merges orderbook level updates into a sorted book.
Last-write-wins by timestamp per price; snapshot kind advances a watermark
that prunes older levels; zero sizes are dropped.
Incoming price arrays must be strictly ascending with no duplicates.
    )";
    FunctionDocumentation::Syntax syntax = "mergeOrderbook(prices, sizes, ts, kind)";
    FunctionDocumentation::Arguments arguments = {
        {"prices", "Sorted unique price levels.", {"Array(Decimal128(19))", "Array(Int128)"}},
        {"sizes", "Parallel TripleSize values.", {"Array(Tuple(Bool, Decimal128(19), Decimal128(19)))"}},
        {"ts", "Event timestamp.", {"DateTime64(6)"}},
        {"kind", "delta or snapshot.", {"Enum8"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Tuple(snapshot_ts, Array(Tuple(price, level_ts, size_triple))).",
        {"Tuple"},
    };
    FunctionDocumentation::Examples examples;
    FunctionDocumentation::IntroducedIn introduced_in = {26, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation
        = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    AggregateFunctionProperties properties
        = {.returns_default_when_only_null = true, .is_order_dependent = true};
    factory.registerFunction("mergeOrderbook", {createAggregateFunctionMergeOrderbook, documentation, properties});
}

}
