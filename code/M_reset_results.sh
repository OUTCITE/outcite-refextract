for index in cioffi; do #geocite cioffi; do
    for tool in anystyle_references_from_cermine_fulltext anystyle_references_from_cermine_refstrings anystyle_references_from_grobid_fulltext anystyle_references_from_grobid_refstrings anystyle_references_from_gold_fulltext anystyle_references_from_gold_refstrings anystyle_references_from_pdftotext_fulltext cermine_references_from_cermine_xml cermine_references_from_grobid_refstrings cermine_references_from_gold_refstrings; do #grobid_references_from_grobid_xml exparser_references_from_cermine_layout merged_references; do
        python3 code/M_add_field.py $index     results_${tool} None  overwrite;
        python3 code/M_add_field.py $index has_results_${tool} False overwrite;
    done;
done
