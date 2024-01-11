DOCINDEX=$1

COLLECTION=ssoar
CONTEXT="model/CONTEXT_${COLLECTION}.json"
METAFILE="${COLLECTION}_GWS"
MODEL="${METAFILE}_new"

LOGFILE=logs/${DOCINDEX}.out
ERRFILE=logs/${DOCINDEX}.err

cd /home/outcite/refextract/

> $LOGFILE
> $ERRFILE

echo "...creates a new SQLITE database containing the latest metadata from the GWS SSOAR index. The code comes from the SKG repository."
python3 code/SKG1_DB_ssoar.py model/MAP_${MODEL}.txt temp/${MODEL}.db gws >>$LOGFILE 2>>$ERRFILE
echo
echo "...creates a new JSON file corresponding to the SQLITE database. The code comes from the SKG repository."
python3 code/SKG2_JSON_ssoar.py temp/${MODEL}.db $CONTEXT resources/ssoar_meta/${METAFILE}.json gws >>$LOGFILE 2>>$ERRFILE
echo
echo "...updates the OUTCITE SSOAR index by adding entries from the JSON file for those SSOAR documents that are not already in it."
python3 code/0_update_index.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE
echo
echo "...updates the OUTCITE SSOAR index by setting the maximum field limit to 5000."
bash code/set_field_limit.sh $DOCINDEX >>$LOGFILE 2>>$ERRFILE
echo
echo "...downloads the PDF for all new SSOAR documents that have been stored in the OUTCITE SSOAR index and stores the link to its local HTTP address in this index"
python3 code/1_update_pdfs.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE
echo
echo "...sends all (previously unprocessed) PDFs to GROBID service running in a separate Screen session for them to be processed into XMLs with all kinds of extracted information in them."
echo "   The XMLs are stored in the OUTCITE SSOAR index together with the metadata and PDF addresses for which they were produced."
python3 code/2_update_xml.py $DOCINDEX 8000 16 >>$LOGFILE 2>>$ERRFILE
echo
echo "...extracts the reference objects from the GROBID-produced XML and stores them with the respective entries in the OUTCITE SSOAR index."
python3 code/G_update_grobid_refs.py $DOCINDEX 16 >>$LOGFILE 2>>$ERRFILE
#echo
#echo "...calls Cermine for all (previously unprocessed) PDFs for them to be processed into XMLs with all kinds of extracted information in them."
#echo "   The XMLs are stored in the OUTCITE SSOAR index together with the metadata and PDF addresses for which they were produced."
#python3 code/C1b_update_cermine_xml.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE
#echo
#echo "...extracts using Cermine the reference objects from previously unprocessed PDFs and stores them with the respective entries in the OUTCITE SSOAR index."
#python3 code/C2_update_cermine_refs.py $DOCINDEX cermine 16 >>$LOGFILE 2>>$ERRFILE
#echo
#echo "...parses using Cermine the reference strings found in the GROBID-produced XML and stores them with the respective entries in the OUTCITE SSOAR index."
#python3 code/C2_update_cermine_refs.py $DOCINDEX grobid 16 >>$LOGFILE 2>>$ERRFILE
#echo
#echo "...parses using Anystyle the reference strings found in the Cermine-produced XML and stores them with the respective entries in the OUTCITE SSOAR index."
#python3 code/A1b_update_anystyle_refs_from_refstrings.py $DOCINDEX cermine 16 >>$LOGFILE 2>>$ERRFILE
echo
echo "...parses using Anystyle the reference strings found in the GROBID-produced XML and stores them with the respective entries in the OUTCITE SSOAR index."
python3 code/A1b_update_anystyle_refs_from_refstrings.py $DOCINDEX grobid 16 >>$LOGFILE 2>>$ERRFILE
echo
echo "...extracts the fulltext portion from the GROBID-produced XML and stores it with the respective entries in the OUTCITE SSOAR index."
python3 code/3_update_fulltext.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE
#echo
#echo "...uses Cermine to extract the fulltext from previously unprocessed PDFs and stores it with the respective entries in the OUTCITE SSOAR index."
#python3 code/C1c_update_cermine_fulltext.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE
echo
echo "...uses PDFtoTXT to extract the fulltext from previously unprocessed PDFs and stores it with the respective entries in the OUTCITE SSOAR index."
python3 code/P_update_pdftotext_fulltext.py $DOCINDEX 16 >>$LOGFILE 2>>$ERRFILE
#echo
#echo "...uses Anystyle to extract and parse references in the fulltext provided by Cermine"
#python3 code/A1a_update_anystyle_refs_from_fulltext.py $DOCINDEX cermine 16 >>$LOGFILE 2>>$ERRFILE
echo
echo "...uses Anystyle to extract and parse references in the fulltext provided by GROBID"
python3 code/A1a_update_anystyle_refs_from_fulltext.py $DOCINDEX grobid 16 >>$LOGFILE 2>>$ERRFILE
echo
echo "...uses Anystyle to extract and parse references in the fulltext provided by PDFtoTXT"
python3 code/A1a_update_anystyle_refs_from_fulltext.py $DOCINDEX pdftotext 16 >>$LOGFILE 2>>$ERRFILE
#echo
#echo "...uses Cermine to create a layout for each unprocessed PDF and stores it with the respective entry in the OUTCITE SSOAR index"
#python3 code/C1a_update_cermine_layout.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE
#echo
#echo "...uses ExParser to extract and parse references in the layout provided by Cermine"
#python3 code/E_update_exparser_refs.py $DOCINDEX >>$LOGFILE 2>>$ERRFILE
echo
echo "...openalex searches in OpenAlex for a matching document given any unprocessed entry in the OUTCITE SSOAR index"
echo "   and when it finds one, obtains the reference list annotated for it in OpenAlex"
python3 code/R_update_references_by_matching.py $DOCINDEX openalex >>$LOGFILE 2>>$ERRFILE
echo
echo "...crossref searches in Crossref for a matching document given any unprocessed entry in the OUTCITE SSOAR index"
echo "   and when it finds one, obtains the reference list annotated for it in Crossref"
python3 code/R_update_references_by_matching.py $DOCINDEX crossref >>$LOGFILE 2>>$ERRFILE
