# DBFTables

[![Build Status](https://travis-ci.org/JuliaData/DBFTables.jl.svg?branch=master)](https://travis-ci.org/JuliaData/DBFTables.jl)

[![Coverage Status](https://coveralls.io/repos/JuliaData/DBFTables.jl/badge.svg?branch=master&service=github)](https://coveralls.io/github/JuliaData/DBFTables.jl?branch=master)

[![codecov.io](http://codecov.io/github/JuliaData/DBFTables.jl/coverage.svg?branch=master)](http://codecov.io/github/JuliaData/DBFTables.jl?branch=master)

For reading [.dbf](https://en.wikipedia.org/wiki/.dbf) files in Julia.

#### Usage

```julia
using DBFTables
io = open("test.dbf")
df = DBFTables.read_dbf(io)
```