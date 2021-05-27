CREATE OR REPLACE FORCE VIEW VW_RECAD_PROCURACAO AS
SELECT DISTINCT
           B.NUM_PRONTUARIO         AS PRONTUARIO,
           P.NOM_PESSOA_FISICA      AS NOME,
           P.NUM_CPF                AS CPF,
           P.DAT_NASC               AS DATA_NASCIMENTO,
           B.DAT_INI_BENEFICIO      AS DATA_CONCESSAO,
           FNC_FORMATA_TELEFONE(P.NUM_TEL_FONE1) AS TELEFONE,
           TIPOLOG.NOM_TIPO         AS TIPO_LOGRADOURO,
           R.NOM_LOGRADOURO       AS LOGRADOURO,
           R.NUM_NUMERO           AS NUMERO,
           BAIRRO.NOM_BAIRRO        AS BAIRRO,
           MUNICIPIO.NOM_ABREVIADO  AS MUNICIPIO,
           R.NUM_CEP              AS CEP,
           ESTADO.NAME              AS ESTADO,
           R.NOM_PESSOA_FISICA      AS NOME_REPRESENTANTE,
           R.NUM_CPF                AS CPF_REPRESENTANTE,
           FNC_FORMATA_TELEFONE(R.NUM_TEL_FONE1) AS TEL_REPRESENTANTE

    FROM RECENSEAMENTO.TB_PESSOA_FISICA P

        INNER JOIN RECENSEAMENTO.TB_BENEFICIARIO B
            ON ( P.COD_IDE_CLI =  B.COD_IDE_CLI )

        INNER JOIN RECENSEAMENTO.TB_REPRESENTANTE_LEGAL R
            ON ( R.COD_IDE_CLI =  P.COD_IDE_CLI )

        LEFT JOIN RECENSEAMENTO.TB_END_TIPO_LOGRADOURO TIPOLOG
            ON (TIPOLOG.ID =  R.COD_TIP_LOGRADOURO_ID)

        LEFT JOIN RECENSEAMENTO.TB_END_BAIRRO BAIRRO
            ON (R.COD_BAIRRO_ID = BAIRRO.ID)

        LEFT JOIN RECENSEAMENTO.TB_DOMINIO_DETALHE ESTADO
            ON (    ESTADO.DOMAIN_ID = 3
                AND ESTADO.ID = R.COD_UF_ID )

        LEFT JOIN RECENSEAMENTO.TB_END_LOCALIDADE MUNICIPIO
            ON (R.COD_MUNICIPIO_ID = MUNICIPIO.ID)
   WHERE nvl(P.FLG_EXCLUIDO,'N') <> 'S';
