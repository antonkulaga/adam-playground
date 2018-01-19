package comp.bio.aging.playground.extras.uniprot


/**
1. UniProtKB-AC
2. UniProtKB-ID
3. GeneID (EntrezGene)
4. RefSeq
5. GI
6. PDB
7. GO
8. UniRef100
9. UniRef90
10. UniRef50
11. UniParc
12. PIR
13. NCBI-taxon
14. MIM
15. UniGene
16. PubMed
17. EMBL
18. EMBL-CDS
19. Ensembl
20. Ensembl_TRS
21. Ensembl_PRO
22. Additional PubMed
  */
case class UniprotMapping(
                           uniprot_ac: String,
                           uniprot_id: String,
                           entrez: String,
                           refSeq: String,
                           gi: String,
                           pdb: String,
                           go: String,
                           uniref100: String,
                           uniref90: String,
                           uniref50: String,
                           uniparc: String,
                           pir: String,
                           taxon: String,
                           mim: String,
                           unigene: String,
                           pubmed: String,
                           embl: String,
                           embl_cds: String,
                           ensembl: String,
                           ensembl_trs: String,
                           ensembl_pro: String,
                           additional_pubmed: String
                         )
{
  lazy val annotations = go.split(";").map(_.trim).toSet
}