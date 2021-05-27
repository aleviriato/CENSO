CREATE OR REPLACE FORCE VIEW VW_SITU_RECADAST2 AS
SELECT   B.NUM_PRONTUARIO         AS PRONTUARIO,
           P.NOM_PESSOA_FISICA      AS NOME_BENEFICIARIO,
           P.NUM_CPF                AS CPF_BENEFICIARIO,
           P.DAT_NASC               AS DATA_NASC_BENEFICIARIO,
           B.DAT_CONCESSAO          AS DATA_CONCESSAO,
           P.EMAIL                  AS EMAIL,
           E.NAME                   AS ORGAO,
           BENEFICIO.NAME           AS TIPO_BENEFICIO,
           DECODE(US.FLAG_FINAL_PRE_CAD, 1, 'CONCLUIDO', 'PENDENTE')        AS RECADASTRAMENTO_ONLINE,
           DECODE(PRO.COD_IDE_CLI, P.COD_IDE_CLI, 'CONCLUIDO', 'PENDENTE')  AS RECADASTRAMENTO_PRESENCIAL,
           max(ATE.DAT_FIN_ATEND)   AS DATA_ATUALIZACAO,
           MOTIVO.NAME              AS MOTIVO,
           (COUNT (DISTINCT A.ID) + COUNT (DISTINCT AH.ID )) AS QTD_AGENDAMENTO,
           COUNT (DISTINCT LOG.ID ) AS QTD_ACESSO,
           US.FLAG_FINAL_PRE_CAD    AS FLAG_FINAL_PRE_CAD,
           P.COD_IDE_CLI            AS COD_IDE_CLI

    FROM RECENSEAMENTO.TB_PESSOA_FISICA P

        INNER JOIN RECENSEAMENTO.TB_BENEFICIARIO B
            ON ( P.COD_IDE_CLI =  B.COD_IDE_CLI )

        INNER JOIN RECENSEAMENTO.TB_USUARIO US
            ON ( P.ID   =  US.PESSOA_FISICA_ID )

        LEFT JOIN RECENSEAMENTO.TB_AGENDAMENTO A
               ON ( P.ID =  A.PESSOA_FISICA_ID )

        LEFT JOIN RECENSEAMENTO.TB_ENTIDADE E
               ON ( B.COD_ENTIDADE  =  E.ID )

        LEFT JOIN RECENSEAMENTO.TB_DOMINIO_DETALHE BENEFICIO
               ON (    BENEFICIO.DOMAIN_ID = 21
                AND BENEFICIO.COD_DOMAIN_DETAIL = B.COD_TIPO_BENEFICIO )

        LEFT JOIN RECENSEAMENTO.TB_LOG LOG
            ON  ( US.ID = LOG.USUARIO_ID )

        LEFT JOIN RECENSEAMENTO.TB_AGENDAMENTO_HISTORICO AH
            ON ( P.ID = AH.PESSOA_FISICA_ID )

        LEFT JOIN RECENSEAMENTO.TB_PROTOCOLO PRO
            ON ( P.COD_IDE_CLI = PRO.COD_IDE_CLI )

           LEFT JOIN (select SERVIDOR_ID,COD_MOT_NAO_FINALIZADO, DAT_FIN_ATEND
                     from RECENSEAMENTO.TB_ATENDIMENTO A1
                    where DAT_INI_ATEND = (select max(DAT_INI_ATEND)
                                             from RECENSEAMENTO.TB_ATENDIMENTO A2
                                            where A2.SERVIDOR_ID = A1.SERVIDOR_ID)) ATE
            ON ( P.ID = ATE.SERVIDOR_ID )

        LEFT JOIN RECENSEAMENTO.TB_DOMINIO_DETALHE MOTIVO
            ON (    MOTIVO.DOMAIN_ID = 20
                AND MOTIVO.COD = ATE.COD_MOT_NAO_FINALIZADO )
    WHERE nvl(P.FLG_EXCLUIDO,'N') <> 'S' AND
         ((US.FLAG_FINAL_PRE_CAD = 1 AND  dat_final_pre_cad >= to_date('17/12/2018','dd/mm/yyyy')) OR US.FLAG_FINAL_PRE_CAD IS NULL OR US.FLAG_FINAL_PRE_CAD = 0)
    GROUP BY B.NUM_PRONTUARIO,
             P.NOM_PESSOA_FISICA,
             P.NUM_CPF,
             P.DAT_NASC,
             B.DAT_CONCESSAO,
             BENEFICIO.NAME,
             MOTIVO.NAME,
             DECODE(US.FLAG_FINAL_PRE_CAD, 1, 'CONCLUIDO', 'PENDENTE'),
             DECODE(PRO.COD_IDE_CLI, P.COD_IDE_CLI, 'CONCLUIDO', 'PENDENTE'),
             E.NAME,
             P.EMAIL,
             US.FLAG_FINAL_PRE_CAD,
             P.COD_IDE_CLI
    ORDER BY TO_CHAR(P.DAT_NASC, 'MM/DD'), P.NOM_PESSOA_FISICA;

