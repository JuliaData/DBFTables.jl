module DBFTables

import Tables, WeakRefStrings
using Dates
using Printf: @sprintf

"Field/column descriptor, part of the Header"
struct FieldDescriptor
    name::Symbol
    type::Type
    dbf_type::Char
    length::UInt8
    ndec::UInt8
end

"Create FieldDescriptor from a column in a Tables.jl table."
function FieldDescriptor(name::Symbol, data::AbstractVector)
    T = Base.nonmissingtype(eltype(data))
    char = dbf_type(T)
    ndec = 0x00
    itr = skipmissing(data)
    if char === 'D'
        len = 0x08
    elseif T === Union{}  # data is only missings
        len = 0x01
    elseif char === 'C'
        width = maximum(x -> ncodeunits(string(x)), itr)
        width > 254 && error("String data must be <254 characters due to DBF limitations.  Found: $width.")
        len = UInt8(width)
    elseif char === 'N'
        len = UInt8(20)
        ndec = T <: AbstractFloat ? 0x1 : 0x0
    elseif char === 'L'
        len = 0x1
    else
        error("This shouldn't be reachable.  Unknown DBF type code: '$char'.")
    end
    FieldDescriptor(name, T, char, len, ndec)
end

"DBF header, which also holds all field definitions"
struct Header
    version::UInt8
    last_update::Date
    records::UInt32
    hsize::UInt16
    rsize::UInt16
    incomplete::Bool
    encrypted::Bool
    mdx::Bool
    lang_id::UInt8
    fields::Vector{FieldDescriptor}
    fieldcolumns::Dict{Symbol,Int}
end

"Struct representing the DBF Table"
struct Table
    header::Header
    data::Vector{UInt8}  # WeakRefString references this
    strings::WeakRefStrings.StringArray{String,2}
end

"Struct representing a single row or record of the DBF Table"
struct Row <: Tables.AbstractRow
    table::Table
    row::Int
end

#-----------------------------------------------------------------------# conversions: Julia-to-DBF
# These are the only types DBFTables.jl will use to save data as.
"Get the DBF type code from the Julia type.  Assumes `Base.nonmissingtype(T)` is the input."
dbf_type(::Type{<:Union{Char, AbstractString}}) = 'C'
dbf_type(::Type{Bool}) = 'L'
dbf_type(::Type{<:Integer}) = 'N'
dbf_type(::Type{<:AbstractFloat}) = 'N'
dbf_type(::Type{Date}) = 'D'
dbf_type(::Type{Union{}}) = 'C'
function dbf_type(::Type{T}) where {T}
    @warn "No DBF type associated with Julia type $T.  Data will be saved as `string(x)`."
    'C'
end

dbf_value(field::FieldDescriptor, val) = dbf_value(Val(field.dbf_type), field.length, val)

# String (or any other type that gets mapped to 'C')
function dbf_value(::Val{'C'}, len::UInt8, x)
    s = string(x)
    out = s * '\0' ^ (len - ncodeunits(s))
    ncodeunits(out) > 254 ? error("The DBF format cannot save strings >254 characters.") : out
end
dbf_value(::Val{'C'}, len::UInt8, ::Missing) = '\0' ^ len

# Bool
dbf_value(::Val{'L'}, ::UInt8, x::Bool) = x ? 'T' : 'F'
dbf_value(::Val{'L'}, ::UInt8, ::Missing) = '?'

# Integer
function dbf_value(::Val{'N'}, ::UInt8, x::Integer)
    s = rpad(x, 20)
    length(s) > 20 ? error("The DBF format cannot save integers >20 characters.") : s
end

# AbstractFloat
function dbf_value(::Val{'N'}, ::UInt8, x::AbstractFloat)
    s = rpad(x, 20)
    length(s) == 20 && return s
    # Force into scientific notation with 20 decimal places
    s2 = @sprintf "%.20e" x
    i = findfirst('e', s2)
    s_end = replace(s2[i:end], '+' => "")
    len = length(s_end)
    n = 20 - len
    out = s2[1:n] * s_end
    @warn "A DBF limitation has reduced the precision of $x by $(length(s) - 20) digits."
    return out
end
dbf_value(::Val{'N'}, ::UInt8, ::Missing) = ' ' ^ 20

# Date
dbf_value(::Val{'D'}, ::UInt8, x::Date) = Dates.format(x, "yyyymmdd")
dbf_value(::Val{'D'}, ::UInt8, ::Missing) = ' ' ^ 8

dbf_value(::Val, ::UInt8, x) = error("This should be unreachable.  No known conversion from Julia to DBF: $x.")

#-----------------------------------------------------------------------# conversions: DBF-to-Julia
"Get the Julia type from the DBF type code and the decimal count"
julia_type(::Val{'C'}, ndec::UInt8) = String
julia_type(::Val{'D'}, ndec::UInt8) = Date
julia_type(::Val{'N'}, ndec::UInt8) = ndec > 0 ? Float64 : Int
julia_type(::Val{'F'}, ndec::UInt8) = Float64
julia_type(::Val{'O'}, ndec::UInt8) = Float64
julia_type(::Val{'I'}, ndec::UInt8) = Int32
julia_type(::Val{'+'}, ndec::UInt8) = Int64
julia_type(::Val{'L'}, ndec::UInt8) = Bool
julia_type(::Val{'M'}, ndec::UInt8) = String
function julia_type(::Val{T}, ndec::UInt8) where {T}
    @warn "Unknown DBF type code '$T'.  Data will be loaded as `String"
    String
end


julia_value(o::FieldDescriptor, s::AbstractString) = julia_value(o.type, Val(o.dbf_type), s::AbstractString)

function julia_value_string(s::AbstractString)
    all(==('\0'), s) ? missing : strip(x -> isspace(x) || x == '\0', s)
end

julia_value(::Type{String}, ::Val{'C'}, s::AbstractString) = julia_value_string(s)
julia_value(::Type{String}, ::Val{'M'}, s::AbstractString) = julia_value_string(s)
function julia_value(::Type{Date}, ::Val{'D'}, s::AbstractString)
    all(isspace, s) ? missing : Date(s, dateformat"yyyymmdd")
end
julia_value(::Type{Int}, ::Val{'N'}, s::AbstractString) = miss(tryparse(Int, s))
julia_value(::Type{Float64}, ::Val{'N'}, s::AbstractString) = miss(tryparse(Float64, s))
julia_value(::Type{Float64}, ::Val{'F'}, s::AbstractString) = miss(tryparse(Float64, s))
# 'O', 'I', and '+' do not use string representations.
function julia_value(::Type{Float64}, ::Val{'O'}, s::AbstractString)
    try
        only(reinterpret(Float64, Vector{UInt8}(s)))
    catch
        missing
    end
end
function julia_value(::Type{Int32}, ::Val{'I'}, s::AbstractString)
    try
        only(reinterpret(Int32, Vector{UInt8}(s)))
    catch
        missing
    end
end
function julia_value(::Type{Int64}, ::Val{'+'}, s::AbstractString)
    try
        only(reinterpret(Int64, Vector{UInt8}(s)))
    catch
        missing
    end
end
function julia_value(::Type{Bool}, ::Val{'L'}, s::AbstractString)
    char = only(s)
    if char in "YyTt"
        return true
    elseif char in "NnFf"
        return false
    else
        return missing
    end
end

"Read a field descriptor from the stream, and create a FieldDescriptor struct"
function read_dbf_field(io::IO)
    n_bytes_field_name = 11 # field name can be up to 11 bytes long, delimited by '\0' (end of string, EOS)
    field_name_bytes = read(io, n_bytes_field_name)
    pos_eos = findfirst(iszero, field_name_bytes)
    n = pos_eos === nothing ? n_bytes_field_name : pos_eos - 1
    field_name = Symbol(field_name_bytes[1:n])

    field_type = read(io, Char)
    skip(io, 4)  # skip
    field_len = read(io, UInt8)
    field_dec = read(io, UInt8)
    skip(io, 14)  # reserved
    jltype = julia_type(Val(field_type), field_dec)
    return FieldDescriptor(field_name, jltype, field_type, field_len, field_dec)
end

reserved(n) = fill(0x00, n)

function Base.write(io::IO, fd::FieldDescriptor)
    out = 0
    out += Base.write(io, replace(rpad(String(fd.name), 11), ' ' => '\0'))  # 0-10
    out += Base.write(io, fd.dbf_type)  # 11
    out += Base.write(io, reserved(4))  # 12-15
    out += Base.write(io, fd.length)  # 16
    out += Base.write(io, fd.ndec)  # 17
    out += Base.write(io, reserved(14))  # 18-31
    return out
end

"Read a DBF header from a stream"
function Header(io::IO)
    ver = read(io, UInt8)
    yy = read(io, UInt8)
    mm = read(io, UInt8)
    dd = read(io, UInt8)
    last_update = Date(yy + 1900, mm, dd)
    records = read(io, UInt32)
    hsize = read(io, UInt16)
    rsize = read(io, UInt16)
    skip(io, 2)  # reserved
    incomplete = Bool(read(io, UInt8))
    encrypted = Bool(read(io, UInt8))
    skip(io, 12)  # reserved
    mdx = Bool(read(io, UInt8))
    lang_id = read(io, UInt8)
    skip(io, 2)  # reserved
    fields = FieldDescriptor[]

    # use Dict for quicker column index lookup
    fieldcolumns = Dict{Symbol,Int}()
    col = 1
    while !eof(io)
        field = read_dbf_field(io)
        fieldcolumns[field.name] = col
        push!(fields, field)
        col += 1

        # peek if we are at the end
        mark(io)
        trm = read(io, UInt8)
        if trm == 0xD
            break
        else
            reset(io)
        end
    end

    return Header(
        ver,
        last_update,
        records,
        hsize,
        rsize,
        incomplete,
        encrypted,
        mdx,
        lang_id,
        fields,
        fieldcolumns,
    )
end



# ref: https://www.clicketyclick.dk/databases/xbase/format/dbf.html
function Base.write(io::IO, h::Header)
    out = 0
    out += Base.write(io, h.version)  # 0
    yy = UInt8(year(h.last_update) - 1900)
    mm = UInt8(month(h.last_update))
    dd = UInt8(day(h.last_update))
    out += Base.write(io, yy, mm, dd)  # 1-3
    out += Base.write(io, h.records)  # 4-7
    out += Base.write(io, h.hsize)  # 8-9
    out += Base.write(io, h.rsize)  # 10-11
    out += Base.write(io, reserved(2))  # 12-13 reserved
    out += Base.write(io, h.incomplete)  # 14
    out += Base.write(io, h.encrypted)  # 15
    out += Base.write(io, reserved(12))  # 16-19, 20-27 reserved
    out += Base.write(io, h.mdx)  # 28
    out += Base.write(io, h.lang_id)  # 29
    out += Base.write(io, reserved(2))  # 30-31 reserved
    for field in h.fields
        out += Base.write(io, field)
    end
    out += Base.write(io, 0xD)
    return out
end


miss(x) = ifelse(x === nothing, missing, x)

# define get functions using getfield since we overload getproperty
"Access the header of a DBF Table"
getheader(dbf::Table) = getfield(dbf, :header)
getfields(dbf::Table) = getheader(dbf).fields
getstrings(dbf::Table) = getfield(dbf, :strings)
getrow(row::Row) = getfield(row, :row)
gettable(row::Row) = getfield(row, :table)

Base.length(dbf::Table) = Int(getheader(dbf).records)
Base.size(dbf::Table) = (length(dbf), length(getfields(dbf)))
Base.size(dbf::Table, i) = size(dbf)[i]
Base.size(row::Row) = (length(row),)
Base.size(row::Row, i) = i == 1 ? length(row) : throw(BoundsError(row, i))

"""
    DBFTables.Table(source) => DBFTables.Table

Read a source, a path to a file or an opened stream, to a DBFTables.Table.
This type conforms to the Tables interface, so it can be easily converted
to other formats. It is possible to iterate through the rows of this object,
or to retrieve columns like `dbf.fieldname`.
"""
function Table(io::IO)
    header = Header(io)
    # consider using mmap here for big dbf files

    # Make sure data is read at the right position
    bytes_to_skip = header.hsize - position(io)
    bytes_to_skip > 0 && skip(io, bytes_to_skip)

    data = Vector{UInt8}(undef, header.rsize * header.records)
    read!(io, data)
    strings = _create_stringarray(header, data)
    Table(header, data, strings)
end

function Table(path::AbstractString)
    open(path) do io
        Table(io)
    end
end

"Collect all the offsets and lengths from the header to create a StringArray"
function _create_stringarray(header::Header, data::AbstractVector)
    # first make the lengths and offsets for a single record
    lengths_record = UInt32.(getfield.(header.fields, :length))
    offsets_record = vcat(0, cumsum(lengths_record)[1:end-1]) .+ 1

    # the lengths are equal for each record
    lengths = repeat(lengths_record, 1, header.records)
    # the offsets accumulate over records with the record size
    row_offsets = range(0; length = header.records, step = header.rsize)
    offsets = repeat(offsets_record, 1, header.records)
    offsets .+= reshape(row_offsets, 1, :)

    WeakRefStrings.StringArray{String,2}(data, offsets, lengths)
end

"Create a NamedTuple representing a single row"
function Base.NamedTuple(row::Row)
    dbf = gettable(row)
    str = getstrings(dbf)
    fields = getfields(dbf)
    ncol = length(fields)
    rowidx = getrow(row)
    @inbounds record = @view str[:, rowidx]
    @inbounds prs = (fields[col].name => julia_value(fields[col], record[col]) for col = 1:ncol)
    return (; prs...)
end

function Base.show(io::IO, row::Row)
    show(io, NamedTuple(row))
end

function Base.show(io::IO, dbf::Table)
    nr, nc = size(dbf)
    println(io, "DBFTables.Table with $nr rows and $nc columns")
    println(io, Tables.schema(dbf))
end

Base.isempty(dbf::Table) = getheader(dbf).records == 0

"Get a BitVector which is true for rows that are marked as deleted"
function isdeleted(dbf::Table)
    data = getfield(dbf, :data)
    rsize = getheader(dbf).rsize
    nrow = getheader(dbf).records
    idx = range(1, step = rsize, length = nrow)
    data[idx] .== 0x2a
end

"Check if the row is marked as deleted"
function isdeleted(dbf::Table, row::Integer)
    data = getfield(dbf, :data)
    i = (row - 1) * getheader(dbf).rsize + 1
    data[i] == 0x2a
end

"Iterate over the rows of a DBF Table, yielding a DBFTables.Row for each row"
function Base.iterate(dbf::Table, st = 1)
    st > length(dbf) && return nothing
    return Row(dbf, st), st + 1
end

function Tables.getcolumn(row::Row, name::Symbol)
    dbf = gettable(row)
    header = getheader(dbf)
    str = getstrings(dbf)
    colidx = get(header.fieldcolumns, name, nothing)
    colidx === nothing && throw(ArgumentError("Column not present: $name"))
    field = @inbounds getfields(dbf)[colidx]
    rowidx = getrow(row)
    return @inbounds julia_value(field, str[colidx, rowidx])
end

function Tables.getcolumn(row::Row, i::Int)
    dbf = gettable(row)
    str = getstrings(dbf)
    field = getfields(dbf)[i]
    rowidx = getrow(row)
    return @inbounds julia_value(field, str[i, rowidx])
end

Tables.istable(::Type{Table}) = true
Tables.rowaccess(::Type{Table}) = true
Tables.columnaccess(::Type{Table}) = true
Tables.rows(dbf::Table) = dbf
Tables.columns(dbf::Table) = dbf

"Get the Tables.Schema of a DBF Table"
function Tables.schema(dbf::Table)
    names = Tuple(field.name for field in getfields(dbf))
    # since missing is always supported, add it to the schema types
    types = Tuple(Union{field.type,Missing} for field in getfields(dbf))
    Tables.Schema(names, types)
end

"List all available DBF column names"
Base.propertynames(dbf::Table) = getfield.(getfield(dbf, :header).fields, :name)
Tables.columnnames(row::Row) = propertynames(gettable(row))

"Create a copy of an entire DBF column as a Vector. Usage: `dbf.myfield`"
function Base.getproperty(dbf::Table, name::Symbol)
    header = getheader(dbf)
    col = get(header.fieldcolumns, name, nothing)
    col === nothing && throw(ArgumentError("Column not present: $name"))
    nrow = header.records
    @inbounds field = getfields(dbf)[col]
    str = getstrings(dbf)
    FT = field.type
    FV = Val{field.dbf_type}()
    return @inbounds Union{FT, Missing}[julia_value(FT, FV, str[col, i]) for i = 1:nrow]
end


Base.write(io::IO, dbf::Table) = Base.write(io, getfield(dbf, :header), getfield(dbf, :data), 0x1a)
Base.write(path::AbstractString, dbf::Table) = open(io -> Base.write(io, dbf), touch(path), "w")


"Generic .dbf writer for the Tables.jl interface."
write(path::AbstractString, tbl) = (open(io -> write(io, tbl), touch(path), "w"); path)

function write(io::IO, tbl)
    dct = Tables.dictcolumntable(tbl)
    fields = [FieldDescriptor(k, v) for (k,v) in pairs(getfield(dct, :values))]
    records = UInt32(length(first(dct)))
    fieldcolumns = Dict{Symbol,Int}(f.name => i for (i,f) in enumerate(fields))
    hsize = UInt16(length(fields) * 32 + 32 + 1) # +1 for the 0xD delimiter
    rsize = UInt16(sum(x -> x.length, fields)) + 1

    version = 0x03
    last_update = today()
    incomplete = false
    encrypted = false
    mdx = false
    lang_id = 0x00

    h = Header(version, last_update, records, hsize, rsize, incomplete, encrypted, mdx, lang_id, fields, fieldcolumns)
    out = Base.write(io, h)

    for row in Tables.rows(dct)
        out += write_record(io, fields, row)
    end
    out += Base.write(io, 0x1a)  # EOF marker
    return out
end

function write_record(io::IO, fd::Vector{FieldDescriptor}, row)
    out = 0
    out += Base.write(io, ' ')  # deletion marker ' '=valid, '*'=deleted
    for (field, val) in zip(fd, row)
        out += Base.write(io, dbf_value(field, val))
    end
    return out
end



end # module
