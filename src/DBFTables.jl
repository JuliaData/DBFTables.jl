module DBFTables

import Tables, WeakRefStrings
using Dates

"Field/column descriptor, part of the Header"
struct FieldDescriptor
    name::Symbol
    type::Type
    dbf_type::Char
    length::UInt8
    ndec::UInt8
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

"Convert DBF data type characters to Julia types"
function typemap(fld::Char, ndec::UInt8)
    # https://www.clicketyclick.dk/databases/xbase/format/data_types.html
    rt = Nothing
    if fld == 'C'
        rt = String
    elseif fld == 'D'
        rt = Date
    elseif fld == 'N'
        if ndec > 0
            rt = Float64
        else
            rt = Int
        end
    elseif fld == 'F' || fld == 'O'
        rt = Float64
    elseif fld == 'I' || fld == '+'
        rt = Int
    elseif fld == 'L'
        rt = Bool
    else
        throw(ArgumentError("Unknown record type $fld"))
    end
    return rt
end

"Read a field descriptor from the stream, and create a FieldDescriptor struct"
function read_dbf_field(io::IO)
    field_name_raw = String(read!(io, Vector{UInt8}(undef, 11)))
    field_name = Symbol(strip(replace(field_name_raw, '\0' => ' ')))
    field_type = read(io, Char)
    skip(io, 4)  # skip
    field_len = read(io, UInt8)
    field_dec = read(io, UInt8)
    skip(io, 14)  # reserved
    jltype = typemap(field_type, field_dec)
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
function Base.Base.write(io::IO, h::Header)
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

"Concert a DBF entry string to a Julia value"
function dbf_value(::Type{Bool}, str::AbstractString)
    char = first(str)
    if char in "YyTt"
        true
    elseif char in "NnFf"
        false
    elseif char == '?'
        missing
    else
        throw(ArgumentError("Unknown logical entry: $(repr(char))"))
    end
end

dbf_value(::Type{Date}, str::AbstractString) = all(isspace, str) ? missing : Date(str, dateformat"yyyymmdd")

dbf_value(T::Union{Type{Int},Type{Float64}}, str::AbstractString) = miss(tryparse(T, str))
# String to avoid returning SubString{String}
function dbf_value(::Type{String}, str::AbstractString)
    stripped = rstrip(str)
    if isempty(stripped)
        # return missing rather than ""
        return missing
    else
        return String(stripped)
    end
end
dbf_value(::Type{Nothing}, ::AbstractString) = missing

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
    @inbounds prs =
        (fields[col].name => dbf_value(fields[col].type, record[col]) for col = 1:ncol)
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
    type = @inbounds getfields(dbf)[colidx].type
    rowidx = getrow(row)
    return @inbounds dbf_value(type, str[colidx, rowidx])
end

function Tables.getcolumn(row::Row, i::Int)
    dbf = gettable(row)
    str = getstrings(dbf)
    type = getfields(dbf)[i].type
    rowidx = getrow(row)
    return @inbounds dbf_value(type, str[i, rowidx])
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
    @inbounds type = getfields(dbf)[col].type
    str = getstrings(dbf)
    @inbounds colarr = [dbf_value(type, str[col, i]) for i = 1:nrow]
    return colarr
end


Base.write(io::IO, dbf::Table) = Base.write(io, getfield(dbf, :header), getfield(dbf, :data), 0x1a)
Base.write(path::AbstractString, dbf::Table) = open(io -> Base.write(io, dbf), touch(path), "w")


"Generic .dbf writer for the Tables.jl interface."
write(path::AbstractString, tbl) = open(io -> write(io, tbl), touch(path), "w")

function write(io::IO, tbl)
    dct = Tables.dictcolumntable(tbl)
    fields, records = get_field_descriptors(dct)
    fieldcolumns = Dict{Symbol,Int}(f.name => i for (i,f) in enumerate(fields))
    hsize = UInt16(length(fields) * 32 + 32)
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

function get_field_descriptors(dct)
    fields = FieldDescriptor[]
    sch = Tables.schema(dct)
    for (name, type) in zip(sch.names, sch.types)
        ndec = 0x0
        len = 0x0
        dbf_type = 'C'
        T = Base.nonmissingtype(type)
        if T isa Date
            dbf_type = 'D'
            len = 0x08
        elseif T <: AbstractString
            # TODO: support memos.  Currently strings > 254 bytes will error
            len = UInt8(maximum(x -> length(string(x)), dct[name]))
            if len > 254
                @warn "Strings will be truncated to 254 characters."
                len = 254
            end
            dbf_type = 'C'
        elseif type === Float64
            dbf_type = 'O'
            len = 0x08
            ndec = 0x01
        elseif T <: AbstractFloat
            dbf_type = 'F'
            len = UInt8(20)
            ndec = 0x01
        elseif T <: Bool
            dbf_type = 'L'
            len = 0x1
        elseif T <: Date
            dbf_type = 'D'
            len = 0x8
        elseif T <: Integer
            dbf_type = 'N'
            len = UInt8(maximum(x -> length(string(x)), dct[name]))
        else
            @warn "Field $name has no known matching DBF data type for $T.  Data will be stored as the DBF character data type ('C')."
            len = UInt8(maximum(x -> length(string(x)), dct[name]))
        end
        push!(fields, FieldDescriptor(name, type, dbf_type, len, ndec))
    end
    fields, UInt32(length(first(dct)))
end

function write_record(io::IO, fd::Vector{FieldDescriptor}, row)
    out = 0
    out += Base.write(io, ' ')  # deletion marker ' '=valid, '*'=deleted
    for (field, val) in zip(fd, row)
        out += Base.write(io, _val(field, val))
    end
    return out
end

function _val(field::FieldDescriptor, val)::Union{String, Float64}
    char = field.dbf_type
    if char == 'L'
        ismissing(val) && return "?"
        val ? "T" : "F"
    elseif ismissing(val)
        ' ' ^ field.length
    elseif char == 'C'
        replace(rpad(val, field.length), !isascii => ' ')
    elseif char == 'D'
        Dates.format(val, "yyyymmdd")
    elseif char == 'O'
        val  # <-- the Float64 return value
    elseif char == 'F'
        rpad(val, 20)[1:20]
    elseif char == 'N'
        rpad(val, field.length)
    else
        error("Unknown DBF datatype $char.")
    end
end

end # module
