CREATE OR REPLACE FORCE VIEW VW_AGENDAMENTO_PERIODO AS
SELECT   l.nome               AS nom_local,
         a.dat_agendamento    AS dat_agendamento,
         a.hora_inicio        AS horario,
         b.num_prontuario     AS prontuario,
         p.nom_pessoa_fisica  AS nome,
         p.num_cpf            AS cpf,
         p.dat_nasc           AS dat_nasc ,
         FNC_FORMATA_TELEFONE(p.num_tel_fone1) AS fone
    FROM RECENSEAMENTO.TB_AGENDAMENTO a
    INNER JOIN RECENSEAMENTO.TB_PESSOA_FISICA p
            ON a.pessoa_fisica_id = p.id
     LEFT JOIN RECENSEAMENTO.TB_BENEFICIARIO B
            ON ( P.COD_IDE_CLI =  B.COD_IDE_CLI )
     LEFT JOIN RECENSEAMENTO.TB_LOCAL L
            ON l.ID = a.local_id
    WHERE nvl(P.FLG_EXCLUIDO,'N') <> 'S'
    ORDER BY TO_DATE(TO_CHAR(a.dat_agendamento, 'DD/MM/YYYY') || a.hora_inicio, 'DD/MM/YYYY HH24:MI') ASC;

