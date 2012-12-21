A = load '$DB.$TABLE' using org.apache.hcatalog.pig.HCatLoader();
B = FILTER A BY $FILTER;
C = foreach B generate foo, bar;
store C into '$OUTPUT_DB.$OUTPUT_TABLE' USING org.apache.hcatalog.pig.HCatStorer('$OUTPUT_PARTITION');
