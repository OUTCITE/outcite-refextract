index=$1 #TODO: Test for geocite, ssoar_gold
specifier='';
if [ $index = 'cioffi' ]; then
    specifier='_cioffi';
fi

while IFS= read -r filename; do
    python3 code/M_parse_gold${specifier}.py resources/gold_references_${index}/refobjects_xml/${filename} resources/gold_references_${index}/refobjects/${filename};
done < resources/gold_references_${index}/docs.txt
