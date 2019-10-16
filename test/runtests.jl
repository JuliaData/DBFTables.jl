using DBFTables
using Test
using Tables
using DataFrames

test_dbf_path = joinpath(@__DIR__, "test.dbf")
dbf = DBFTables.Table(test_dbf_path)
df = DataFrame(dbf)
row, st = iterate(dbf)

@testset "DBFTables" begin
    @testset "DataFrame indexing" begin
        @test size(df, 1) == 7 # records
        @test size(df, 2) == 6 # fields
        @test df[2, :CHAR] == "John"
        @test df[1, :DATE] == "19900102"
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
        @test h.last_update == "20140806"
        @test h.records == 7
        @test length(h.fields) == 6
    end

    @testset "show" begin
        @test sprint(show, row) === sprint(show, NamedTuple(row))
        @test sprint(
            show,
            dbf,
        ) === "DBFTables.Table with 7 rows and 6 columns\nTables.Schema:\n :CHAR     Union{Missing, String} \n :DATE     Union{Missing, String} \n :BOOL     Union{Missing, Bool}   \n :FLOAT    Union{Missing, Float64}\n :NUMERIC  Union{Missing, Float64}\n :INTEGER  Union{Missing, $Int}  \n"
    end

    @testset "iterate" begin
        @test st === 2
        @test row.CHAR === "Bob"
        @test_throws ArgumentError row.nonexistent_field
        firstrow = (
            CHAR = "Bob",
            DATE = "19900102",
            BOOL = false,
            FLOAT = 10.21,
            NUMERIC = 11.21,
            INTEGER = 100,
        )
        @test NamedTuple(row) === firstrow
        @test row isa DBFTables.Row
        @test DBFTables.getrow(row) === 1
        @test DBFTables.gettable(row) === dbf
        @test sum(1 for row in dbf) === 7
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
