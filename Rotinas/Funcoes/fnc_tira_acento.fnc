CREATE OR REPLACE FUNCTION "FNC_TIRA_ACENTO"
(pString IN VARCHAR2) RETURN VARCHAR2 is
--
vStringReturn varchar2(2000);

--
begin
  vStringReturn := translate( pString,
                    'ֱַֹֽ׃ÚְָּׂÙֲÊ־װÛֳױֻÜבחיםףתאטלעשגךמפûדץכü',
                    'ACEIOUAEIOUAEIOUAOEUaceiouaeiouaeiouaoeu');
  --
  return vStringReturn;
  --
end;
/

