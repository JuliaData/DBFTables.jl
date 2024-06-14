using DBFTables
using Test
using Tables
using DataFrames
using Dates

#-----------------------------------------------------------------------------# setup
df = DataFrame(
    a = [1,2,3],
    b = ["one", "two", "three"],
    c = [true, true, false],
    d = [today() + Day(i) for i in 1:3],
    e = 1.0:3.0,
    f = ["😄", "∱", "∫eˣ123"]
)

# Same as above but with missings
df2 = vcat(df, DataFrame([missing missing missing missing missing missing], names(df)))

# `df2` as a DBFTables.Table
dbf = DBFTables.Table(DBFTables.write(tempname(), df2))

# `dbf` after write/read roundtrip
dbf2 = DBFTables.Table(DBFTables.write(tempname(), dbf))

# Check that data survives a write/read roundtrip
function roundtrip(t)
    path = DBFTables.write(tempname(), t)
    t2 = DBFTables.Table(path)
    isequal(DataFrame(t), DataFrame(t2))
end

#-----------------------------------------------------------------------------# tests
@testset "DBFTables" begin
    @testset "DBFTables.Table roundtrip" begin
        @test Tables.schema(df2) == Tables.schema(dbf)
        @test Tables.schema(dbf) == Tables.schema(dbf2)
        @test isequal(NamedTuple.(dbf), NamedTuple.(dbf2))

        @test DBFTables.isdeleted(dbf2) isa BitVector
        @test all(.!DBFTables.isdeleted(dbf2))
        @test !DBFTables.isdeleted(dbf2, 3)

        @test ismissing(dbf2.a[end])
    end

    @testset "Tables.jl roundtrips" begin
        @test roundtrip(df)
        @test roundtrip(df2)
        @test roundtrip(dbf)
        @test roundtrip([(x=Float32(1), y=1), (x=Float32(2), y=2), (x=missing, y=3)])
        @test roundtrip([(x=true, y="test"), (x=missing, y=missing)])
        @test roundtrip([(x=today(), y=missing), (x=missing, y=today())])
        @test roundtrip([(; x=1.0), (; x=missing)])
        @test roundtrip([(; x=missing), (; x=missing)])

        @test_warn "No DBF type associated with Julia type Vector{Float64}" DBFTables.write(tempname(), [(; x = rand(5))])
        @test_throws Exception DBFTables.write(tempname(), [(; x = rand(999))])
    end

    @testset "Header" begin
        # Verify that Header survives write/read roundtrip
        h, h2 = getfield(dbf, :header), getfield(dbf2, :header)
        for name in fieldnames(DBFTables.Header)
            @test getfield(h, name) == getfield(h2, name)
        end
    end

    @testset "show" begin
        str = """
        DBFTables.Table with 4 rows and 6 columns
        Tables.Schema:
         :a  Union{Missing, $Int}
         :b  Union{Missing, String}
         :c  Union{Missing, Bool}
         :d  Union{Missing, Date}
         :e  Union{Missing, Float64}
         :f  Union{Missing, String}
        """
        @test sprint(show, dbf) == str
    end

    @testset "iterate and other Base methods" begin
        @test size(dbf) == size(df2)
        @test size(dbf, 1) == size(df2, 1)
        @test size(dbf, 2) == size(df2, 2)
        for row in dbf
            @test_throws ArgumentError row.nonexistent_field
            @test length(row) == length('a':'f')
            @test size(row) == (length(row), )
            @test size(row, 1) == length(row)
            @test propertynames(row) == Symbol.('a':'f')
            for prop in propertynames(row)
                @test getproperty(row, prop) isa Any # dummy test to ensure no error is thrown
            end
        end

        @test sum(1 for row in dbf) === 4
        @test sum(1 for cell in first(dbf)) === 6
    end

    @testset "Numeric 20-character Limit Nonsense" begin
        big = BigInt(99999_99999_99999_99999)
        @test DBFTables.dbf_value(Val('N'), 0x01, big) == string(big)
        @test_throws Exception DBFTables.dbf_value(Val('N'), 0x01, big + 1)

        negbig = -BigInt(99999_99999_99999_9999)  # one less digit for the minus sign
        @test DBFTables.dbf_value(Val('N'), 0x01, negbig) == string(negbig)
        @test_throws Exception DBFTables.dbf_value(Val('N'), 0x01, negbig - 1)

        @test_warn "DBF limitation" DBFTables.dbf_value(Val('N'), 0x01, prevfloat(Inf))
        @test_warn "DBF limitation" DBFTables.dbf_value(Val('N'), 0x01, nextfloat(-Inf))
    end
end
