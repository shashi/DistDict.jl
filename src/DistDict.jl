module DistDict

using ComputeFramework
import ComputeFramework: split_range, PartitionScheme, alignfirst, DomainBranch, project, isempty, intersect, domain, cat_data

immutable DictDomain <: Domain
    hashrange::UnitRange
end

Base.isempty(d::DictDomain) = isempty(d.hashrange)
Base.intersect(d::DictDomain, d2::DictDomain) =
    DictDomain(intersect(d.hashrange, d2.hashrange))
project(d::DictDomain, d2::DictDomain) = d
Base.getindex(bigd::DictDomain, d::DictDomain) = d
alignfirst(a::DictDomain) = a

function Base.getindex(dict::Dict, d::DictDomain)
    res = Dict()
    for k in keys(dict)
        if hash(k) in d.hashrange
            res[k] = dict[k]
        end
    end
    res
end

immutable DictPartition <: PartitionScheme
    bins::Int
end

function ComputeFramework.partition(p::DictPartition, d::DictDomain)
    rs = collect(1:UInt(typemax(UInt)/p.bins+1):typemax(UInt))
    ranges = map(UnitRange, rs[1:end-1], rs[2:end])
    ComputeFramework.DomainBranch(d, map(DictDomain, ranges))
end

domain(x::Dict) = DictDomain(zero(UInt):(typemax(UInt)-1))

cat_data(x::DictPartition, dmn::DomainBranch, parts::AbstractArray) = merge(parts...)

Base.map(f, d::Dict) = Dict(Pair[k=>f(v) for (k,v) in d])

end # module
