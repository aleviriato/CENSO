CREATE OR REPLACE FUNCTION FNC_FORMATA_TELEFONE
(
  DES_NUM_TEL IN VARCHAR2 
) RETURN VARCHAR2 AS 
  V_TELEFONE      VARCHAR2(50);
  V_TELEFONE_COMP VARCHAR2(50);
BEGIN
   
   V_TELEFONE := DES_NUM_TEL;
   V_TELEFONE := REGEXP_REPLACE(V_TELEFONE, '[^0-9]', '');
   V_TELEFONE_COMP := regexp_replace(V_TELEFONE, '(^[0]{1})', '');
   --dbms_output.put_line('TELEFONE: ' || V_TELEFONE);
   
   IF    (LENGTH(V_TELEFONE_COMP) = 11) THEN
         V_TELEFONE := regexp_replace(V_TELEFONE, '(^[0]{1})', '');
         V_TELEFONE := regexp_replace(V_TELEFONE,'(^[[:digit:]]{2})([[:digit:]]{5})([[:digit:]]{4}$)','(\1) \2-\3');
   ELSIF (LENGTH(V_TELEFONE_COMP) = 10) THEN
         V_TELEFONE := regexp_replace(V_TELEFONE, '(^[0]{1})', '');
         V_TELEFONE := regexp_replace(V_TELEFONE,'(^[[:digit:]]{2})([[:digit:]]{4})([[:digit:]]{4}$)','(\1) \2-\3');
   ELSIF (LENGTH(V_TELEFONE_COMP) = 8) THEN
         V_TELEFONE := regexp_replace(V_TELEFONE, '(^[0]{1})', '');
         V_TELEFONE := regexp_replace(V_TELEFONE,'(^[[:digit:]]{4})([[:digit:]]{4}$)','\1-\2');
   ELSE 
         V_TELEFONE := V_TELEFONE;
   END IF;
   
  RETURN V_TELEFONE;
    
END FNC_FORMATA_TELEFONE;
/

