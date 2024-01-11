codedir=/home/outcite/deployment/refextract/code/
logdir=/home/outcite/deployment/refextract/logs/

codedir_matching=/home/outcite/deployment/data_matching/code/
logdir_matching=/home/outcite/deployment/data_matching/logs/

codedir_linking=/home/outcite/deployment/data_linking/code/
logdir_linking=/home/outcite/deployment/data_linking/logs/

codedir_searching=/home/outcite/deployment/data_searching/code/
logdir_searching=/home/outcite/deployment/data_searching/logs/

doimapfolder=/home/outcite/data_linking/resources/;

mkdir -p $logdir
mkdir -p $logdir_matching
mkdir -p $logdir_linking
mkdir -p $logdir_searching

i=0
for task in index pdfs xml fulltext; do
    echo $task;
    python3 ${codedir}${i}_update_${task}.py users 8003 >${logdir}users_${task}.out 2>${logdir}users_${task}.err;
    sleep 3
    ((i++))
done

echo grobid refs;
python3 ${codedir}G_update_grobid_refs.py users 2 >${logdir}users_grobid_refs.out 2>${logdir}users_grobid_refs.err;

echo cermine layout from pdf;
python3 ${codedir}C1a_update_cermine_layout.py users >${logdir}users_cermine_layout.out     2>${logdir}users_cermine_layout.err;
echo cermine xml from pdf;
python3 ${codedir}C1b_update_cermine_xml.py users >${logdir}users_cermine_xml.out           2>${logdir}users_cermine_xml.err;
echo cermine fulltext from pdf;
python3 ${codedir}C1c_update_cermine_fulltext.py users >${logdir}users_cermine_fulltext.out 2>${logdir}users_cermine_fulltext.err;

echo pdftotext fulltext from pdf;
python3 ${codedir}P_update_pdftotext_fulltext.py users >${logdir}users_pdftotext_fulltext.out 2>${logdir}users_pdftotext_fulltext.err;

echo cermine refs from cermine refstrs
python3 ${codedir}C2_update_cermine_refs.py users cermine >${logdir}users_cermine_cermine.out 2>${logdir}users_cermine_cermine.err;
echo cermine refs from grobid refstrs
python3 ${codedir}C2_update_cermine_refs.py users grobid >${logdir}users_cermine_grobid.out   2>${logdir}users_cermine_grobid.err;

echo anystyle refs from cermine refstrs
python3 ${codedir}A1b_update_anystyle_refs_from_refstrings.py users cermine >${logdir}users_anystyle_cermine.out 2>${logdir}users_anystyle_cermine.err;
echo anystyle refs from grobid refstrs
python3 ${codedir}A1b_update_anystyle_refs_from_refstrings.py users grobid  >${logdir}users_anystyle_grobid.out  2>${logdir}users_anystyle_grobid.err;

echo anystyle refs from cermine fulltext
python3 ${codedir}A1a_update_anystyle_refs_from_fulltext.py users cermine    >${logdir}users_anystyle_cermine_full.out    2>${logdir}users_anystyle_cermine_full.err;
echo anystyle refs from grobid fulltext
python3 ${codedir}A1a_update_anystyle_refs_from_fulltext.py users grobid     >${logdir}users_anystyle_grobid_full.out     2>${logdir}users_anystyle_grobid_full.err;
echo anystyle refs from pdftotext fulltext
python3 ${codedir}A1a_update_anystyle_refs_from_fulltext.py users pdftotext  >${logdir}users_anystyle_pdftotext_full.out  2>${logdir}users_anystyle_pdftotext_full.err;
echo done.

#echo exparser refs from cermine layout
#python3 ${codedir}E_update_exparser_refs.py users >${logdir}users_exparser.out 2>${logdir}users_exparser.err;

for target in openalex crossref; do
    echo references by matching via ${target};
    python3 ${codedir}R_update_references_by_matching.py users ${target} >${logdir}users_ref_match_${target}.out 2>${logdir}users_ref_match_${target}.err;
done

for target in sowiport crossref dnb openalex ssoar arxiv econbiz gesis_bib research_data; do
    echo matching to ${target}
    python3 ${codedir_matching}update_${target}.py users >${logdir_matching}users_${target}.out  2>${logdir_matching}users_${target}.err;
done

for target in sowiport crossref dnb openalex ssoar arxiv econbiz gesis_bib research_data; do
    echo linking to ${target}
    python3${codedir_linking}update_${target}.py users >${logdir_linking}users_${target}.out  2>${logdir_linking}users_${target}.err;
done

for target in research_data openalex econbiz; do
    echo getting dois from matches to ${target};
    python3 ${codedir_linking}update_target_dois.py users ${target} >${logdir_linking}users_dois_${target}.out  2>${logdir_linking}users_dois_${target}.err;
done

echo getting dois from matches to crossref;
python3 ${codedir_linking}update_crossref_dois.py users >${logdir_linking}users_dois_crossref.out  2>${logdir_linking}users_dois_crossref.err;

for target in ssoar arxiv research_data openalex econbiz crossref; do
    echo getting general url from ${target} doi;
    python3 ${codedir_linking}update_general_url.py users ${target} >${logdir_linking}users_general_url_${target}.out  2>${logdir_linking}users_general_url_${target}.err;
    for database in core unpaywall; do
        echo getting pdf url from ${target} doi via ${database};
        python3 ${codedir_linking}update_pdf_url.py users ${doimapfolder}${database}.db ${target} >${logdir_linking}users_pdf_url_${database}_${target}.out 2>${logdir_linking}users_pdf_url${database}_${target}.err;
    done;
done

for database in core unpaywall; do
    echo getting pdf url from extacted doi via ${database};
    python3 ${codedir_linking}update_pdf_url.py users ${doimapfolder}${database}.db >${logdir_linking}users_pdf_url_${database}.out 2>${logdir_linking}users_pdf_url${database}.err;
done;
echo getting general url from extracted doi;
python3 ${codedir_linking}update_general_url.py users >${logdir_linking}users_general_url.out  2>${logdir_linking}users_general_url.err;

#echo matching and linking to bing
#python3 ${codedir_searching}update_bing.py users >${logdir_searching}users_bing.out  2>${logdir_searching}users_bing.err;
