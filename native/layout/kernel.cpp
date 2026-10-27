// Offline-only bounded matching and placement. Never opens a chain database.
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <vector>

extern "C" int match_round(int64_t n, int64_t edges, int dims,
    const int64_t* a, const int64_t* b, const int64_t* size,
    const double* demand, const double* limit, int max_size, int64_t* partner) {
    std::fill(partner, partner+n, -1);
    for (int64_t e=0; e<edges; ++e) {
        int64_t x=a[e], y=b[e];
        if(x<0 || y<0 || x>=n || y>=n || x==y) return 1;
        if(partner[x]>=0 || partner[y]>=0 || size[x]+size[y]>max_size) continue;
        bool valid=true;
        for(int c=0;c<dims;++c)
            if(demand[x*dims+c]+demand[y*dims+c]>limit[c]+1e-12) valid=false;
        if(valid) { partner[x]=y; partner[y]=x; }
    }
    return 0;
}

// demand is normalized to one shard's mean for each nonempty coordinate.
// Lexicographic potential: overload beyond band, edge cut, squared load.
extern "C" int place(int64_t n, const int64_t* off, const int32_t* peer,
    const double* weight, const double* demand, int dims, const int64_t* order,
    int shards, double band, int rounds, int32_t* route, double* trace,
    int64_t* moves) {
    if(n<1 || shards<2 || dims<1 || rounds<0 || band<1) return 1;
    std::vector<double> loads(shards*dims,0), affinity(shards,0);
    std::vector<int64_t> population(shards,0);
    std::fill(route,route+n,-1);
    auto quality=[&](int from,int to,int64_t g) {
        double peak=0, square=0;
        for(int s=0;s<shards;++s) for(int c=0;c<dims;++c) {
            double z=loads[s*dims+c]+((s==to)-(s==from))*demand[g*dims+c];
            peak=std::max(peak,z); square+=z*z;
        }
        return std::pair<double,double>(std::max(0.0,peak-band),square);
    };
    auto better=[](double a,double b,double c,double x,double y,double z) {
        const double eps=1e-10;
        if(a<x-eps) return true;
        if(std::abs(a-x)>eps) return false;
        if(b<y-eps) return true;
        if(std::abs(b-y)>eps) return false;
        return c<z-eps;
    };
    for(int64_t pos=0;pos<n;++pos) {
        int64_t g=order[pos];
        if(g<0 || g>=n) return 2;
        std::fill(affinity.begin(),affinity.end(),0);
        for(int64_t e=off[g];e<off[g+1];++e)
            if(route[peer[e]]>=0) affinity[route[peer[e]]]+=weight[e];
        int best=-1; double ba=0,bb=0,bc=0;
        for(int s=0;s<shards;++s) {
            auto q=quality(-1,s,g);
            if(best<0 || better(q.first,-affinity[s],q.second,ba,bb,bc) ||
               (q.first==ba && -affinity[s]==bb && q.second==bc && population[s]<population[best])) {
                best=s;ba=q.first;bb=-affinity[s];bc=q.second;
            }
        }
        route[g]=best; ++population[best];
        for(int c=0;c<dims;++c) loads[best*dims+c]+=demand[g*dims+c];
    }
    auto record=[&](int r) {
        double cut=0,peak=0,square=0;
        for(int64_t g=0;g<n;++g) for(int64_t e=off[g];e<off[g+1];++e)
            if(route[g]!=route[peer[e]]) cut+=weight[e]*.5;
        for(double x:loads){peak=std::max(peak,x);square+=x*x;}
        trace[3*r]=std::max(0.0,peak-band);trace[3*r+1]=cut;trace[3*r+2]=square;
    };
    record(0);
    for(int r=0;r<rounds;++r) {
        moves[r]=0;
        for(int64_t pos=0;pos<n;++pos) {
            int64_t g=order[pos]; int from=route[g],best=from;
            std::fill(affinity.begin(),affinity.end(),0);
            for(int64_t e=off[g];e<off[g+1];++e) affinity[route[peer[e]]]+=weight[e];
            auto base=quality(from,from,g);
            double ba=base.first,bb=0,bc=base.second;
            for(int to=0;to<shards;++to) if(to!=from) {
                auto q=quality(from,to,g);
                double cut_delta=affinity[from]-affinity[to];
                if(better(q.first,cut_delta,q.second,ba,bb,bc)) {
                    best=to;ba=q.first;bb=cut_delta;bc=q.second;
                }
            }
            if(best!=from) {
                for(int c=0;c<dims;++c) {
                    loads[from*dims+c]-=demand[g*dims+c];
                    loads[best*dims+c]+=demand[g*dims+c];
                }
                route[g]=best;++moves[r];
            }
        }
        record(r+1);
    }
    return 0;
}

// A planning-window cohort reaches its closest already-known ancestor once.
extern "C" int ancestors(int64_t n,int64_t known,const int64_t* parent,int64_t* tag) {
    for(int64_t i=0;i<n;++i) {
        if(i<known) {tag[i]=i;continue;}
        int64_t p=parent[i];
        if(p>=i || p< -1) return 1;
        tag[i]=(p<0 ? -1 : tag[p]);
    }
    return 0;
}
