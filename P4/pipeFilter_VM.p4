#include <core.p4>
#include <v1model.p4>

#define COUNTER_LEN 65536
#define COUNTER_HASH_LEN 16

#define A_SHIFT 1
#define B_SHIFT 2
#define C_SHIFT 2

const bit<16> RANDOM_SEED = 16w0x1234;
const bit<16> A_ADD=16w0x1;
const bit<16> B_ADD=16w0x1;
const bit<16> C_ADD=16w0x3;


header Ethernet {
	bit<48> dstAddr;
	bit<48> srcAddr;
	bit<16> etherType;
}


header Ipv4{
	bit<4> version;
	bit<4> ihl;
	bit<8> diffserv;
    bit<16> total_len;
	bit<16> identification;
	bit<3> flags;
	bit<13> fragOffset;
	bit<8> ttl;
    bit<8> protocol;
	bit<16> checksum;
	bit<32> srcAddr;
	bit<32> dstAddr;
}  


header MyFlow{
	bit<32> id;
}

struct ingress_headers_t{
	Ethernet ethernet;
	Ipv4 ipv4;
	MyFlow myflow;
}

struct ingress_metadata_t{
	bit<16> fingerprint;
  	bit<16> index;
	bit<1> flag;
	bit<16> hash_fp;
}

struct egress_headers_t {}
struct egress_metadata_t {}


enum bit<16> ether_type_t {
    IPV4    = 0x0800,
    ARP     = 0x0806
}

enum bit<8> ip_proto_t {
    ICMP    = 1,
    IGMP    = 2,
    TCP     = 6,
    UDP     = 17
}

parser IngressParser(packet_in pkt,
        out ingress_headers_t hdr,
        out ingress_metadata_t metadata,
        inout standard_metadata_t standard_metadata){

    state start{
		pkt.extract(ig_intr_md);
		pkt.advance(PORT_METADATA_SIZE);
		transition parse_ethernet;
	}

    state parse_ethernet{
		pkt.extract(hdr.ethernet);
        transition select((bit<16>)hdr.ethernet.etherType) {
            (bit<16>)ether_type_t.IPV4      : parse_ipv4;
            (bit<16>)ether_type_t.ARP       : accept;
            default : accept;
        }
	}

	state parse_ipv4{
		pkt.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol) {
            (bit<8>)ip_proto_t.ICMP             : accept;
            (bit<8>)ip_proto_t.IGMP             : accept;
            (bit<8>)ip_proto_t.TCP              : parse_myflow;
            (bit<8>)ip_proto_t.UDP              : parse_myflow;
            default : accept;
        }
	}

	state parse_myflow{
		pkt.extract(hdr.myflow);
		transition accept;
	}
}

struct bucket{
	bit<16> cell1;
	bit<16> cell2;
}

control ingress(inout ingress_headers_t hdr,
        inout ingress_metadata_t meta,
        inout standard_metadata_t standard_metadata
        )
{
    register<bit<64>>(COUNTER_LEN) buckets1;
    register<bit<64>>(COUNTER_LEN) buckets2;
    register<bit<64>>(COUNTER_LEN) buckets3;
    register<bit<64>>(COUNTER_LEN) buckets4;


    action generateIF(){
        hash(meta.index,HashAlgorithm.crc32,16w0,{hdr.myflow.id},16w16);
        hash(meta.fingerprint,HashAlgorithm.crc32_custom,16w0,{hdr.myflow.id},16w16);
        meta.flag=1w0x0;
        meta.hash_fp=fingerprint ^ RANDOM_SEED;
    }

    //generate index and fingerprint
	table generateIF_table{
		actions={
			generateIF;
		}
		size = 1;
		const default_action =generateIF();
	}

    action generateNextIndex1(){
		meta.index=(meta.hash_fp-meta.index);
		meta.index=(meta.index>>A_SHIFT)+A_ADD;
	}

	table generateNextIndex1_table{
		actions={
			generateNextIndex1;
		}
		size=1;
		const default_action =generateNextIndex1();
	}
	action generateNextIndex2(){
		meta.index=(meta.hash_fp-meta.index);
		meta.index=(meta.index>>B_SHIFT)+B_ADD;
	}
	table generateNextIndex2_table{
		actions={
			generateNextIndex2;
		}
		size=1;
		const default_action =generateNextIndex2();
	}
	action generateNextIndex3(){
		meta.index=(meta.hash_fp-meta.index);
		meta.index=(meta.index>>C_SHIFT)+C_ADD;
	}
	table generateNextIndex3_table{
		actions={
			generateNextIndex3;
		}
		size=1;
		const default_action =generateNextIndex3();
	}
//stage1
    action insert_buckets1(){
        bit<16> x1;
        bit<16> x2;
        bit<16> x3;
        bit<16> x4;
        bit<64> fp;
        buckets1.read(fp,meta.index);
        x1=fp[15:0];
        x2=fp[31:16];
        x3=fp[47:32];
        x4=fp[63:48];
        if(x1==16w0x0){
            x1=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x2==16w0x0){
            x2=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x3==16w0x0){
            x3=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x4==16w0x0){
            x4=meta.fingerprint;
            meta.flag=1w0x1;
        }

        fp=x4++x3++x2++x1;
        buckets1.write(meta.index,fp);

    }

    table insert_buckets1_table{
        actions={
            insert_buckets1;
        }
        size=1;
		const default_action =insert_buckets1();

    }

//stage2
    action insert_buckets2(){
        bit<16> x1;
        bit<16> x2;
        bit<16> x3;
        bit<16> x4;
        bit<64> fp;
        buckets2.read(fp,meta.index);
        x1=fp[15:0];
        x2=fp[31:16];
        x3=fp[47:32];
        x4=fp[63:48];
        if(x1==16w0x0){
            x1=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x2==16w0x0){
            x2=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x3==16w0x0){
            x3=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x4==16w0x0){
            x4=meta.fingerprint;
            meta.flag=1w0x1;
        }

        fp=x4++x3++x2++x1;
        buckets2.write(meta.index,fp);

    }

    table insert_buckets2_table{
        actions={
            insert_buckets2;
        }
        size=1;
		const default_action =insert_buckets2();

    }

//stage3
    action insert_buckets3(){
        bit<16> x1;
        bit<16> x2;
        bit<16> x3;
        bit<16> x4;
        bit<64> fp;
        buckets3.read(fp,meta.index);
        x1=fp[15:0];
        x2=fp[31:16];
        x3=fp[47:32];
        x4=fp[63:48];
        if(x1==16w0x0){
            x1=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x2==16w0x0){
            x2=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x3==16w0x0){
            x3=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x4==16w0x0){
            x4=meta.fingerprint;
            meta.flag=1w0x1;
        }

        fp=x4++x3++x2++x1;
        buckets3.write(meta.index,fp);

    }

    table insert_buckets3_table{
        actions={
            insert_buckets3;
        }
        size=1;
		const default_action =insert_buckets3();

    }

//stage4
    action insert_buckets4(){
        bit<16> x1;
        bit<16> x2;
        bit<16> x3;
        bit<16> x4;
        bit<64> fp;
        buckets4.read(fp,meta.index);
        x1=fp[15:0];
        x2=fp[31:16];
        x3=fp[47:32];
        x4=fp[63:48];
        if(x1==16w0x0){
            x1=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x2==16w0x0){
            x2=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x3==16w0x0){
            x3=meta.fingerprint;
            meta.flag=1w0x1;
        }
        else if(x4==16w0x0){
            x4=meta.fingerprint;
            meta.flag=1w0x1;
        }

        fp=x4++x3++x2++x1;
        buckets4.write(meta.index,fp);

    }
    table insert_buckets4_table{
        actions={
            insert_buckets4;
        }
        size=1;
		const default_action =insert_buckets4();

    }
    apply{
        if (hdr.myflow.isValid()){
            generateIF_table.apply();
            if(meta.flag==1w0x0){
				insert_buckets1_table.apply();
			}
			if(meta.flag==1w0x0){
                generateNextIndex1_table.apply();
				insert_buckets2_table.apply();
			}
			if(meta.flag==1w0x0){
				generateNextIndex2_table.apply();
				insert_buckets3_table.apply();
			}
			if(meta.flag==1w0x0){
                generateNextIndex3_table.apply();
				insert_buckets4_table.apply();
			}
        }
    }
}

control IngressDeparser(packet_out pkt,
	inout ingress_headers_t hdr)
{
	apply{
		pkt.emit(hdr);
	}
}

control MyEgress(inout ingress_headers_t hdr,
        inout ingress_metadata_t metadata,
                 inout standard_metadata_t standard_metadata) {
    apply {  }
}

V1Switch(IngressParser(), Ingress(), ingress(), IngressDeparser(),MyEgress()) main;

