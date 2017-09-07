using DBFTables
using Base.Test

dir = @__DIR__
# dir = joinpath(Pkg.dir("DBFTables"), "test")
df = DBFTables.read_dbf(joinpath(dir, "test.dbf"))

@test size(df,1) == 3 # records
@test size(df,2) == 6 # fields
@test df[:CHAR][2] == "John"
@test df[:DATE][1] == "19900102"
@test df[:BOOL][3] == false
@test df[:FLOAT][1] == 10.21
@test df[:NUMERIC][2] == 12.21
@test df[:INTEGER][3] == 102