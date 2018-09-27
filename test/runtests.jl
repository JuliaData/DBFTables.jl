using DBFTables
using Test
using Missings

dir = @__DIR__
# dir = joinpath(Pkg.dir("DBFTables"), "test")
df = DBFTables.read_dbf(joinpath(dir, "test.dbf"))

@test size(df,1) == 7 # records
@test size(df,2) == 6 # fields
@test df[:CHAR][2] == "John"
@test df[:DATE][1] == "19900102"
@test df[:BOOL][3] == false
@test df[:FLOAT][1] == 10.21
@test df[:NUMERIC][2] == 12.21
@test df[:INTEGER][3] == 102

# Testing missing record handling
@test ismissing(df[:BOOL][4])
@test ismissing(df[:FLOAT][5])
@test ismissing(df[:NUMERIC][6])
@test ismissing(df[:INTEGER][7])
