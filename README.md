# DBFTables

[![CI](https://github.com/JuliaData/WeakRefStrings.jl/workflows/CI/badge.svg)](https://github.com/JuliaData/WeakRefStrings.jl/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/JuliaData/WeakRefStrings.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaData/WeakRefStrings.jl)
[![deps](https://juliahub.com/docs/DBFTables/deps.svg)](https://juliahub.com/ui/Packages/DBFTables/P7Qdk?t=2)
[![version](https://juliahub.com/docs/DBFTables/version.svg)](https://juliahub.com/ui/Packages/DBFTables/P7Qdk)
[![pkgeval](https://juliahub.com/docs/DBFTables/pkgeval.svg)](https://juliahub.com/ui/Packages/DBFTables/P7Qdk)

Read xBase / dBASE III+ [.dbf](https://en.wikipedia.org/wiki/.dbf) files in Julia. Supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface.

[Shapefile.jl](https://github.com/JuliaGeo/Shapefile.jl) uses this package to read the information associated to the geometries of the `.shp` file.

## Usage

```julia
using DBFTables
dbf = DBFTables.Table("test.dbf")

# whole columns can be retrieved by their name
# note that this creates a copy, so instead of repeated `dbf.field` calls,
# it is faster to once do `field = dbf.field` and then use `field` instead
dbf.INTEGER  # => Union{Missing, Int64}[100, 101, 102, 0, 2222222222, 4444444444, missing]

# example function that iterates over the rows and uses two columns
function sumif(dbf)
    total = 0.0
    for row in dbf
        if row.BOOLEAN && !ismissing(row.NUMERIC)
            value += row.NUMERIC
        end
    end
    return total
end

# for other functionality, convert to other Tables such as DataFrame
using DataFrames
df = DataFrame(dbf)
```

## Format description resources
- https://en.wikipedia.org/wiki/.dbf
- https://www.clicketyclick.dk/databases/xbase/format/dbf.html
- http://www.independent-software.com/dbase-dbf-dbt-file-format.html

## Implementation details

The DBF header contains information on the amount of rows, which columns are present, what type they are, and how many bytes the entries are. Based on this we can create a `Tables.Schema`. Each row is a fixed amount of bytes. All data is represented as strings, with different conventions based on the specified type. There are no delimiters between the entries, but since we know the sizes from the header, it is not needed.

The `DBFTables.Table` struct holds both the header and data. All data is read into memory in one go as a `Vector{UInt8}`. To provide efficient access into the individual entries, we use [WeakRefStrings](https://github.com/JuliaData/WeakRefStrings.jl/). WeakRefStrings' `StringArray` only holds the offsets and lengths into the `Vector{UInt8}` with all the data. Then we still need to convert from the string to the julia type. This is done on demand with `dbf_value`.

Note that the format also contains a "record deleted" flag, which is represented by a `'*'` at the start of the row. When this is encountered the record should be treated as if it doesn't exist. Since normally writers strip these records when writing, they are rarely encountered. For that reason this package ignores these flags by default right now. To check for the flags yourself, there is the `isdeleted` function. A sample file with deleted record flags is available [here](https://issues.qgis.org/issues/11007#note-30).
