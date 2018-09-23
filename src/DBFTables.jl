module DBFTables

using DataFrames, Printf

# Read DBF files in xBase format
# Files written in this format have the extension .dbf
# Implemented: dBase III+ (w/o memo)

struct DBFFieldDescriptor
	nam::String
	typ::Type
	len::Int8
	dec::Int8
end

struct DBFHeader
	version::UInt8
	lastUpdate::String
	records::Int32
	hsize::Int16
	rsize::Int16
	incomplete::Bool
	encrypted::Bool
	mdx::Bool
	langId::UInt8
	fields::Vector{DBFFieldDescriptor}
end

function dbf_field_type(fld::Char, dec::UInt8)
	rt = Nothing
	if fld == 'C'
		rt = String
	elseif fld == 'D'
		rt = String
	elseif fld == 'N'
		if dec > 0
			rt = Float64
		else
			rt = Int
		end
	elseif fld == 'F' || fld == 'O'
		rt = Float64
	elseif fld == 'I' || fld == '+'
		rt = Integer
	elseif fld == 'L'
		rt = Bool
	else
		 warn("Unknown record type: $(fld)")
	end
	return rt
end

function read_dbf_field(io::IO)
	field_name = strip(replace(String(read!(io, Vector{UInt8}(undef, 11))),'\0'=>' ')) # 0x00
	field_type = read(io, Char)  # 0x0B
	read(io, Int32) # skip 0x0C
	field_len = read(io, UInt8) # 0x10
	field_dec = read(io, UInt8) # 0x11
	read!(io, Vector{UInt8}(undef, 14)) # reserved
	return DBFFieldDescriptor(field_name, dbf_field_type(field_type, field_dec), field_len, field_dec)
end

function read_dbf_header(io::IO)
	ver = read(io, UInt8)
	date = read!(io, Vector{UInt8}(undef, 3)) # 0x01
	last_update = @sprintf("%4d%02d%02d", date[1]+1900, date[2], date[3])
	records = read(io, Int32) # 0x04
	hsize = read(io, Int16) # 0x08
	rsize = read(io, Int16) # 0x0A
	read(io, Int16) # reserved # 0x0C
	incomplete = Bool(read(io, UInt8)) # 0x0E
	encrypted = Bool(read(io, UInt8)) # 0x0F
	read!(io, Vector{UInt8}(undef, 12)) # reserved
	mdx = Bool(read(io, UInt8)) # 0x1C
	langId = read(io, UInt8) # 0x1D
	read!(io, Vector{UInt8}(undef, 2)) # reserved # 0x1E
	fields = DBFFieldDescriptor[]

	while !eof(io)
		push!(fields, read_dbf_field(io))
		p = position(io)
		trm = read(io, UInt8)
		if trm == 0xD
			break
		else
			seek(io, p)
		end
	end

	return DBFHeader(ver, last_update, records, hsize, rsize,
					 incomplete, encrypted, mdx, langId,
					 fields)
end

function read_dbf_records!(io::IO, df::DataFrame, header::DBFHeader; deleted=false)
	rc = 0
	while header.records != rc
		is_deleted = (read(io, UInt8) == 0x2A)
		r = Any[]
		for i = 1:length(header.fields)
			#print("P: $(position(io)) ")
			fld_data = read!(io, Vector{UInt8}(undef, header.fields[i].len))
			#println("D: $(ascii(fld_data))")
			if header.fields[i].typ == Bool
				logical = Char(fld_data[1])
				if logical in ['Y', 'y', 'T', 't']
					push!(r, true)
				elseif logical in ['N', 'n', 'F', 'f']
					push!(r, false)
				else
					push!(r, missing)
				end
			elseif header.fields[i].typ == Int
				tmp = tryparse(header.fields[i].typ, String(fld_data))
				push!(r, tmp==nothing ? missing : tmp)
			elseif header.fields[i].typ == Float64
				tmp = tryparse(header.fields[i].typ, String(fld_data))
				push!(r, tmp==nothing ? missing : tmp)
			elseif header.fields[i].typ == String
				push!(r, strip(String(fld_data)))
			elseif header.fields[i].typ == Nothing
				push!(r, missing)
			else
				warn("Type $(header.fields[i].typ) is not supported")
			end
		end
		if !is_deleted || deleted
			push!(df, r)
		end
		rc += 1
		#println("R: $(position(io)), $(eof(io)), $(rc) ")
	end
	return df
end

function read_dbf(io::IO; deleted=false)
    header = read_dbf_header(io)
	df = DataFrame(map(f->Union{f.typ,Missing}, header.fields), map(f->Symbol(f.nam), header.fields), 0)
	read_dbf_records!(io, df, header; deleted=deleted)
	return df
end

function read_dbf(fnm::String; deleted=false)
	io = open(fnm)
	df = read_dbf(io; deleted=deleted)
	close(io)
	return df
end

end # module
