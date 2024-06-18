# DBFTables

[![CI](https://github.com/JuliaData/DBFTables.jl/workflows/CI/badge.svg)](https://github.com/JuliaData/DBFTables.jl/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/JuliaData/DBFTables.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/JuliaData/DBFTables.jl)
[![deps](https://juliahub.com/docs/DBFTables/deps.svg)](https://juliahub.com/ui/Packages/DBFTables/P7Qdk?t=2)
[![version](https://juliahub.com/docs/DBFTables/version.svg)](https://juliahub.com/ui/Packages/DBFTables/P7Qdk)
[![pkgeval](https://juliahub.com/docs/DBFTables/pkgeval.svg)](https://juliahub.com/ui/Packages/DBFTables/P7Qdk)

Read xBase / dBASE III+ [.dbf](https://en.wikipedia.org/wiki/.dbf) files in Julia. Supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface.

[Shapefile.jl](https://github.com/JuliaGeo/Shapefile.jl) uses this package to read the information associated to the geometries of the `.shp` file.

## Usage

```julia
using DBFTables, DataFrames

df = DataFrame(
    x = 1:5,
    y = rand(Bool, 5),
    z = ["a", "b", "c", "d", "e"]
)

# Write any Tables.jl source to a .dbf file
path = DBFTables.write(tempname(), df)

# Read the data back in from the .dbf file
dbf = DBFTables.Table(path)

# Retrieve columns by their name
dbf.x

# Iterate over the rows (values can be accessed by column name)
for row in dbf
    @info (row.x, row.y, row.z)
end

# Pass the DBFTables.Table to any Tables.jl sink
df2 = DataFrame(dbf)
```

## Format description resources
- https://en.wikipedia.org/wiki/.dbf
- https://www.clicketyclick.dk/databases/xbase/format/dbf.html
- http://www.independent-software.com/dbase-dbf-dbt-file-format.html

## Implementation details

The DBF header contains information on the amount of rows, which columns are present, what type they are, and how many bytes the entries are. Based on this we can create a `Tables.Schema`. Each row is a fixed amount of bytes. All data is represented as strings, with different conventions based on the specified type. There are no delimiters between the entries, but since we know the sizes from the header, it is not needed.

The `DBFTables.Table` struct holds both the header and data. All data is read into memory in one go as a `Vector{UInt8}`. To provide efficient access into the individual entries, we use [WeakRefStrings](https://github.com/JuliaData/WeakRefStrings.jl/). WeakRefStrings' `StringArray` only holds the offsets and lengths into the `Vector{UInt8}` with all the data. Then we still need to convert from the string to the julia type. This is done on demand with `dbf_value`.

Note that the format also contains a "record deleted" flag, which is represented by a `'*'` at the start of the row. When this is encountered the record should be treated as if it doesn't exist. Since normally writers strip these records when writing, they are rarely encountered. For that reason this package ignores these flags by default right now. To check for the flags yourself, there is the `isdeleted` function. A sample file with deleted record flags is available [here](https://issues.qgis.org/issues/11007#note-30).


## Quirks and Gotchas

The DBF format is quite old (introduced in 1983).  As such, it has some quirks that may not be immediately obvious:

1. An empty string is equivalent to a missing value.  Thus an empty string in a table will not survive a `write`/`read` round trip.
2. Strings are limited to 254 characters.  Attempting to write longer Strings results in an error.
3. In order to support as many versions of DBF as possible, DBFTables.jl will only write data as one of the following DBF data types:
  - `'C'` (Character): `String`s (and anything else that doesn't doesn't match one of the other three types).
  - `'N'` (Numeric): `Integer`s and `AbstractFloat`s.
  - `'L'` (Logical): `Bool`s.
  - `'D'` (Date): `Date`s.
4. The `'N` (Numeric) data type restricts values to fit within 20 printed characters.  All `Int64`s fit within 20 characters, but `Float64`s may not.  E.g. `string(nextfloat(-Inf))` is 23 characters.  DBFTables.jl will remove the least significant digits (loss of precision) in order to fit within the 20 character limit.
