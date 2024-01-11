index=$1

codedir=/home/outcite/refextract/code/
logdir=/home/outcite/refextract/logs/

mkdir -p $logdir


echo indexing;
python3 ${codedir}0_update_index.py ${index} >${logdir}${index}_index.out 2>${logdir}${index}_index.err;

echo downloading pdfs;
python3 ${codedir}1_update_pdfs.py ${index} >${logdir}${index}_pdfs.out 2>${logdir}${index}_pdfs.err;

echo grobid xml;
python3 ${codedir}2_update_xml.py ${index} >${logdir}${index}_xml.out 8000 16 2>${logdir}${index}_xml.err;

echo grobid fulltext;
python3 ${codedir}3_update_fulltext.py ${index} >${logdir}${index}_fulltext.out 2>${logdir}${index}_fulltext.err;


echo grobid refs;
python3 ${codedir}G_update_grobid_refs.py ${index} >${logdir}${index}_grobid_refs.out 2>${logdir}${index}_grobid_refs.err;

echo cermine layout from pdf;
python3 ${codedir}C1a_update_cermine_layout.py ${index} >${logdir}${index}_cermine_layout.out     2>${logdir}${index}_cermine_layout.err;
echo cermine xml from pdf;
python3 ${codedir}C1b_update_cermine_xml.py ${index} >${logdir}${index}_cermine_xml.out           2>${logdir}${index}_cermine_xml.err;
echo cermine fulltext from pdf;
python3 ${codedir}C1c_update_cermine_fulltext.py ${index} >${logdir}${index}_cermine_fulltext.out 2>${logdir}${index}_cermine_fulltext.err;

echo pdftotext fulltext from pdf;
python3 ${codedir}P_update_pdftotext_fulltext.py ${index} >${logdir}${index}_pdftotext_fulltext.out 2>${logdir}${index}_pdftotext_fulltext.err;

echo cermine refs from cermine refstrs
python3 ${codedir}C2_update_cermine_refs.py ${index} cermine >${logdir}${index}_cermine_cermine.out 2>${logdir}${index}_cermine_cermine.err;
echo cermine refs from grobid refstrs
python3 ${codedir}C2_update_cermine_refs.py ${index} grobid >${logdir}${index}_cermine_grobid.out   2>${logdir}${index}_cermine_grobid.err;

echo anystyle refs from cermine refstrs
python3 ${codedir}A1b_update_anystyle_refs_from_refstrings.py ${index} cermine >${logdir}${index}_anystyle_cermine.out 2>${logdir}${index}_anystyle_cermine.err;
echo anystyle refs from grobid refstrs
python3 ${codedir}A1b_update_anystyle_refs_from_refstrings.py ${index} grobid 16  >${logdir}${index}_anystyle_grobid.out  2>${logdir}${index}_anystyle_grobid.err;

echo anystyle refs from cermine fulltext
python3 ${codedir}A1a_update_anystyle_refs_from_fulltext.py ${index} cermine    >${logdir}${index}_anystyle_cermine_full.out    2>${logdir}${index}_anystyle_cermine_full.err;
echo anystyle refs from grobid fulltext
python3 ${codedir}A1a_update_anystyle_refs_from_fulltext.py ${index} grobid    16 >${logdir}${index}_anystyle_grobid_full.out     2>${logdir}${index}_anystyle_grobid_full.err;
echo anystyle refs from pdftotext fulltext
python3 ${codedir}A1a_update_anystyle_refs_from_fulltext.py ${index} pdftotext 16 >${logdir}${index}_anystyle_pdftotext_full.out  2>${logdir}${index}_anystyle_pdftotext_full.err;
echo done.

echo exparser refs from cermine layout
python3 ${codedir}E_update_exparser_refs.py ${index} >${logdir}${index}_exparser.out 2>${logdir}${index}_exparser.err;

for target in openalex crossref; do
    echo references by matching via ${target};
    python3 ${codedir}R_update_references_by_matching.py ${index} ${target} >${logdir}${index}_ref_match_${target}.out 2>${logdir}${index}_ref_match_${target}.err;
done
