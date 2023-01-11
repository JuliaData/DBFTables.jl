using DBFTables
using Test
using Tables
using DataFrames
using Dates

test_dbf_path = joinpath(@__DIR__, "test.dbf")
dbf = DBFTables.Table(test_dbf_path)
df = DataFrame(dbf)
row, st = iterate(dbf)

@testset "DBFTables" begin
    @testset "Writing" begin
        tables_equal(tbl1, tbl2) = all(zip(Tables.columns(tbl1), Tables.columns(tbl2))) do (t1, t2)
            all(ismissing(a) ? ismissing(b) : a == b for (a,b) in zip(t1,t2))
        end
        function _roundtrip(table)
            file = joinpath(tempdir(), "test.dbf")
            DBFTables.write(file, table)
            table2 = DBFTables.Table(file)
        end
        roundtrip(table) = tables_equal(DataFrame(table), DataFrame(_roundtrip(table)))
        @test roundtrip(df)
        @test roundtrip(dbf)
        @test roundtrip([(x=Float32(1), y=1), (x=Float32(2), y=2), (x=missing, y=3)])
        @test roundtrip([(x=true, y="test"), (x=missing, y=missing)])
        @test roundtrip([(x=today(), y=missing), (x=missing, y=today())])
        @test roundtrip([(; x=1.0), (;x=missing)])

        @test_warn "Data will be stored as the DBF character data type" DBFTables.write(tempname(), [(; x = rand(10))])

        # Base.write for DBFTables.Table
        file = joinpath(tempdir(), "test.dbf")
        write(file, dbf)
        dbf2 = DBFTables.Table(file)
        @test tables_equal(dbf, dbf2)
    end

    @testset "DataFrame indexing" begin
        @test size(df, 1) == 7 # records
        @test size(df, 2) == 6 # fields
        @test df[2, :CHAR] == "John"
        @test df[1, :DATE] == Date("19900102", dateformat"yyyymmdd")
        @test df[3, :BOOL] == false
        @test df[1, :FLOAT] == 10.21
        @test df[2, :NUMERIC] == 12.21
        @test df[3, :INTEGER] == 102
    end

    @testset "missing entries" begin
        @test ismissing(df[4, :BOOL])
        @test ismissing(df[5, :FLOAT])
        @test ismissing(df[6, :NUMERIC])
        @test ismissing(df[7, :INTEGER])
    end

    @testset "header" begin
        h = DBFTables.Header(open(test_dbf_path))
        @test h.version == 3
        @test h.last_update == Date("20140806", dateformat"yyyymmdd")
        @test h.records == 7
        @test length(h.fields) == 6
    end

    @testset "show" begin
        @test sprint(show, row) === sprint(show, NamedTuple(row))
        # use replace to update to julia 1.4 union printing
        @test replace(sprint(show, dbf), r"\} +" => "}") ===
              "DBFTables.Table with 7 rows and 6 columns\nTables.Schema:\n :CHAR     Union{Missing, String}\n :DATE     Union{Missing, Date}\n :BOOL     Union{Missing, Bool}\n :FLOAT    Union{Missing, Float64}\n :NUMERIC  Union{Missing, Float64}\n :INTEGER  Union{Missing, $Int}\n"
    end

    @testset "iterate" begin
        @test st === 2
        @test haskey(row, :CHAR)
        @test row.CHAR === "Bob"
        @test row[2] === Date("19900102", dateformat"yyyymmdd")
        @test_throws ArgumentError row.nonexistent_field
        firstrow = (
            CHAR = "Bob",
            DATE = Date("19900102", dateformat"yyyymmdd"),
            BOOL = false,
            FLOAT = 10.21,
            NUMERIC = 11.21,
            INTEGER = 100,
        )
        @test NamedTuple(row) === firstrow
        @test row isa DBFTables.Row
        @test row isa Tables.AbstractRow
        @test length(row) === 6
        @test size(row) === (6,)
        @test size(row, 1) === 6
        @test_throws BoundsError size(row, 2)
        @test DBFTables.getrow(row) === 1
        @test DBFTables.gettable(row) === dbf
        @test sum(1 for row in dbf) === 7
        @test sum(1 for cell in row) === 6
        @test propertynames(dbf) == [:CHAR, :DATE, :BOOL, :FLOAT, :NUMERIC, :INTEGER]
        @test propertynames(row) == [:CHAR, :DATE, :BOOL, :FLOAT, :NUMERIC, :INTEGER]
    end

    @testset "column" begin
        @test size(dbf) === (7, 6)
        @test size(dbf, 2) === 6

        @test length(dbf.CHAR) === 7
        @test dbf.CHAR isa Vector{Union{String,Missing}}
        @test dbf.INTEGER isa Vector{Union{Int,Missing}}
        @test_throws ArgumentError row.nonexistent_field
        @test dbf.INTEGER[2] === 101
        @test ismissing(dbf.INTEGER[7])
        @test dbf.CHAR[2] === "John"
        @test ismissing(dbf.CHAR[7])

        @test DBFTables.isdeleted(dbf) isa BitVector
        @test all(.!DBFTables.isdeleted(dbf))
        @test !DBFTables.isdeleted(dbf, 3)
    end

end  # testset "DBFTables"
