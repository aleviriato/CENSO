CREATE OR REPLACE FORCE VIEW VW_RECAD_PRESENCIAL AS
SELECT     COUNT (*) AS COUNT,
           REC.NOM_PESSOA_FISICA AS RECADASTRADOR,
           PFS.NOM_PESSOA_FISICA AS NOM_BENEFICIARIO,
           PFS.NUM_CPF           AS CPF_BENEFICIARIO,
           PFS.DAT_NASC          AS DAT_NASCIMENTO,
           TO_CHAR(PRT.DAT_GER_PROT,'DD/MM/YYYY HH24:MI')  AS DAT_PROTOCOLO,
           FNC_FORMATA_TELEFONE(PFS.NUM_TEL_FONE1)        AS TELEFONE,
           PFS.EMAIL             AS EMAIL
    FROM RECENSEAMENTO.TB_PESSOA_FISICA PFS
        INNER JOIN RECENSEAMENTO.TB_BENEFICIARIO  TBE
            ON (TBE.COD_IDE_CLI = PFS.COD_IDE_CLI)
        INNER JOIN RECENSEAMENTO.TB_PROTOCOLO PRT
             ON (PFS.COD_IDE_CLI= PRT.COD_IDE_CLI)
        LEFT JOIN RECENSEAMENTO.TB_USUARIO USU
             ON ( PRT.ID_USU_PROT = USU.ID)
        LEFT JOIN RECENSEAMENTO.TB_PESSOA_FISICA REC
             ON ( USU.PESSOA_FISICA_ID = REC.ID)
    WHERE NVL(PFS.FLG_EXCLUIDO,'N') <> 'S'
    GROUP BY REC.NOM_PESSOA_FISICA,
             PFS.NOM_PESSOA_FISICA,
             PFS.NUM_CPF,
             PFS.DAT_NASC,
             FNC_FORMATA_TELEFONE(PFS.NUM_TEL_FONE1),
             PRT.DAT_GER_PROT,
             PFS.EMAIL
    ORDER BY PFS.NOM_PESSOA_FISICA;
