WITH
KOMBINASJONER_OG_NAVN AS (
  SELECT *
    FROM UNNEST(
      ARRAY<STRUCT<
      navn STRING, appId STRING, platform STRING>>[
        ('LG', 'no.nrk.accedo.tv.lg', 'tv'),
        ('Gammel Tizen', 'no.nrk.accedo.tv.tizen', 'tv'),
        ('Accedo ws', 'no.nrk.accedo.tv.workstation', 'tv'),
        ('iOS', 'no.nrk.nrktvapp', 'mob'),
        ('Apple TV', 'no.nrk.nrktvapp', 'tv'),
        ('Android mob', 'no.nrk.tv', 'mob'),
        ('Android TV', 'no.nrk.tv', 'tv'),
        ('Tizen', 'no.nrk.tv.smart-tv', 'tv'),
        ('web', 'no.nrk.tv.web', 'web')])),

VIEWID_OG_DIREKTESIDE AS (
  /* Denne spørringen samler opp dataene som trengs for å senere kunne bygge stiene som 
     identifiserer turer innom direktesidene.
     Forklaring til feltene
       - navn           : navn på kombinasjonen av plattform og appId
       - viewId og      : gjør det mulig å koble sammen sekvenser av skjermvisninger
         previousViewId 
       - skjerm         : kombinerer mobileViewName eller page.path eventuelt med en kanal,
                          og bearbeider page.path til å ligne mer på mobileViewName,se merknad.
       - sessionId      : identifiserer besøket
       - firstTimeStamp : brukes til å beregne hvor langt ut i besøket skjermvisningen fant sted
       - sub_sesjon     : deler et besøk opp i underbesøk basert på at previousViewId er NULL
       
     Merknad: Kanal er basert på conten.id. Regexp-en filtrerer bort verdier som ikke er av
       typen nrk1, nrk2, nrktv4, nrk_tegnspråk.  
       */
  SELECT DISTINCT IFNULL(navn, 'Annet') navn, viewId, previousViewId,
         IF(appId IN ('no.nrk.tv.smart-tv', 'no.nrk.tv.web'),
           CASE
             WHEN SPLIT(page.path, '/')[SAFE_OFFSET(1)] = 'direkte' AND SPLIT(page.path, '/')[SAFE_OFFSET(3)] = 'avspiller' THEN 
               CASE -- Deler opp tilfellene der vi skal ha med kanal
                 WHEN SPLIT(page.path, '/')[SAFE_OFFSET(2)] LIKE 'nrk1_%' THEN 'avspiller (nrk1)'
                 WHEN REGEXP_CONTAINS(SPLIT(page.path, '/')[SAFE_OFFSET(2)], r'^nrk(s.+|_|tv\d|\d)+') THEN CONCAT('avspiller (', SPLIT(page.path, '/')[SAFE_OFFSET(2)] , ')')
               END
             WHEN SPLIT(page.path, '/')[SAFE_OFFSET(1)] = 'direkte' THEN 
               CASE
                 WHEN SPLIT(page.path, '/')[SAFE_OFFSET(2)] LIKE 'nrk1_%' THEN 'direkte (nrk1)'
                 WHEN REGEXP_CONTAINS(SPLIT(page.path, '/')[SAFE_OFFSET(2)], r'^nrk(s.+|_|tv\d|\d)+') THEN CONCAT('direkte (', SPLIT(page.path, '/')[SAFE_OFFSET(2)] , ')')
                 ELSE 'direkte'
               END
             WHEN SPLIT(page.path, '/')[SAFE_OFFSET(1)] = 'epg' THEN 
               CASE
                 WHEN SPLIT(page.path, '/')[SAFE_OFFSET(2)] LIKE 'nrk1_%' THEN 'epg (nrk1)'
                 WHEN REGEXP_CONTAINS(SPLIT(page.path, '/')[SAFE_OFFSET(2)], r'^nrk(s.+|_|tv\d|\d)+') THEN CONCAT('epg (', SPLIT(page.path, '/')[SAFE_OFFSET(2)] , ')')
                 ELSE 'epg'
               END
             WHEN SPLIT(page.path, '/')[SAFE_OFFSET(1)] != '' THEN SPLIT(page.path, '/')[SAFE_OFFSET(1)]
             WHEN SPLIT(page.path, '/')[SAFE_OFFSET(1)] = '' THEN 'forsiden'
           END,
           CONCAT(mobileViewName,
             CASE
               WHEN content.id LIKE 'nrk1_%' THEN ' (nrk1)'
               WHEN REGEXP_CONTAINS(content.id, r'^nrk(_|tv\d|\d)+') THEN CONCAT(' (', content.id, ')')
               ELSE ''
             END)
         ) skjerm,
         sessionId, firstTimeStamp, 
         COUNTIF(previousViewId IS NULL) OVER(PARTITION BY sessionId ORDER BY firstTimeStamp) sub_sesjon,
    FROM `nrk-datahub.snowplow_processed.views_v02`
         LEFT JOIN KOMBINASJONER_OG_NAVN USING(appId, platform)
   WHERE partitionDate = CURRENT_DATE - 2
     AND nrkService = 'nrktv'
     AND platform != 'pc'
     AND appId NOT IN ('no.nrk.nrktvapp.swift', 'no.nrk.goldendelicious.cdntest', 'no.nrk.NRK-Super')),

/*
   Her beregnes det hvor lang tid det tar fra første skjermvisning i besøket til den gjeldende
   skjermvisningen.
   - delta_t        : tiden det tar fra besøket startet til denne sidevisningen
*/
TIDER AS (
  SELECT *,
         TIMESTAMP_DIFF(firstTimestamp, MIN(firstTimestamp) OVER(PARTITION BY sessionId, sub_sesjon), SECOND) delta_t
    FROM VIEWID_OG_DIREKTESIDE),

ALLE_STIER AS (
  /* Spørringen kobler sammen tre skjermvisninger som kommer etter hverandre (stier) og
     identifiserer tilfeller som er innom direktesiden. Disse grupperes på grunnlag av om 
     neste skjermvisning var avspilling/tv-guide eller noe annet (bounce). I tillegg kate-
     goriseres stiene som en ønsketur eller en bomtur avhenging av om andre skjermvisning
     var avspilling/tv-guide eller ikke.
     Forklaring av nye felter:
     - app             : oversetter appId til et mer forståelig navn
     - sti             : kategoriserer sekvenser av tre skjermvisninger
     - tur             : ønsketur eller bomtur
     - tid_til_steg1-3 : tiden fra start av besøket til direkte-skjermvisning osv. */
  SELECT fv.sessionId, sub_sesjon, navn, fv.skjerm fskjerm, av.skjerm askjerm, tv.skjerm tskjerm,
         CASE
           WHEN av.skjerm IS NULL THEN CONCAT(fv.skjerm, ' -> bounce')
           WHEN tv.skjerm IS NULL THEN CONCAT(fv.skjerm, ' -> ', av.skjerm, ' -> bounce')
           ELSE CONCAT(fv.skjerm, ' -> ', av.skjerm, ' -> ', tv.skjerm)
         END sti,
         CASE
           WHEN REGEXP_CONTAINS(fv.skjerm, r'avspiller \(.+\)') THEN 'Sesjonsstart'
           WHEN REGEXP_CONTAINS(fv.skjerm, r'direkte') THEN 'direktesiden'
           WHEN REGEXP_CONTAINS(fv.skjerm, r'forsiden|frontpage') THEN 'forsiden'
           WHEN REGEXP_CONTAINS(fv.skjerm, r'(tv-guide|epg)( \(.+\))?') THEN 'Sesjonsstart'
           ELSE 'annet'
         END Fra,          
         CASE
           WHEN REGEXP_CONTAINS(fv.skjerm, r'(tv-guide|epg)( \(.+\))?') AND REGEXP_CONTAINS(av.skjerm, r'avspiller \(.+\)') THEN 'tv-guide -> avspiller'
           WHEN REGEXP_CONTAINS(fv.skjerm, r'(tv-guide|epg)( \(.+\))?') AND NOT REGEXP_CONTAINS(av.skjerm, r'(tv-guide|epg)( \(.+\))?') THEN 'tv-guide -> bounce'
           WHEN REGEXP_CONTAINS(av.skjerm, r'(tv-guide|epg)( \(.+\))?') AND REGEXP_CONTAINS(tv.skjerm, r'avspiller \(.+\)') THEN 'tv-guide -> avspiller'
           WHEN REGEXP_CONTAINS(av.skjerm, r'(tv-guide|epg)( \(.+\))?') AND NOT REGEXP_CONTAINS(tv.skjerm, r'(tv-guide|epg)( \(.+\))?') THEN 'tv-guide -> bounce'
           WHEN REGEXP_CONTAINS(fv.skjerm, r'avspiller \(.+\)') THEN 'direkteavspilling'
           WHEN REGEXP_CONTAINS(av.skjerm, r'avspiller \(.+\)') THEN 'direkteavspilling'
           WHEN REGEXP_CONTAINS(av.skjerm, r'(tv-guide|epg)( \(.+\))?') THEN 'tv-guide'
           ELSE 'bounce'
         END Til,          

         fv.firstTimeStamp tid_skjerm1, av.firstTimeStamp tid_skjerm2,
         fv.delta_t tid_til_steg1,
         av.delta_t tid_til_steg2,
    FROM TIDER fv
          LEFT JOIN (SELECT sessionId, previousViewId, firstTimeStamp, viewId, skjerm, delta_t FROM TIDER) av ON fv.viewId = av.previousViewId AND fv.sessionId = av.sessionId
          LEFT JOIN (SELECT sessionId, previousViewId, viewId, skjerm, delta_t FROM TIDER) tv ON av.viewId = tv.previousViewId AND fv.sessionId = tv.sessionId),

GOALS AS (
  SELECT DISTINCT sessionId, navn, Fra, Til, tid_til_steg1, tid_skjerm1, sub_sesjon,
         REGEXP_EXTRACT(sti, r'\((.+?)\)') kanal,
         Til IN ('direkteavspilling', 'tv-guide -> avspiller', 'tv-guide') `hovedmål`,
         (Fra = 'direktesiden' AND Til = 'bounce') OR Til = 'tv-guide -> bounce' `alternativt mål`
    FROM ALLE_STIER
),

FLAGGET AS (
  SELECT DISTINCT sessionId, navn, Fra, Til, tid_til_steg1, `hovedmål`, `alternativt mål`,
         LOGICAL_OR(`hovedmål`) OVER(PARTITION BY sessionId, sub_sesjon) `oppnådd hovedmål`,
         tid_skjerm1 = MIN(tid_skjerm1) OVER(PARTITION BY sessionId, sub_sesjon, `hovedmål`, `alternativt mål`) goal1
    FROM GOALS),

FLAGGET2 AS (
  SELECT DISTINCT *,
         (`oppnådd hovedmål` AND `hovedmål` AND goal1) OR (NOT `oppnådd hovedmål` AND `alternativt mål` AND goal1) flagg
    FROM FLAGGET)

SELECT DISTINCT navn, Fra, Til,
       COUNT(sessionId) OVER(PARTITION BY navn, Fra, Til) `Antall besøk`,
       COUNT(sessionId) OVER(PARTITION BY navn, Fra, Til) / COUNT(sessionId) OVER(PARTITION BY navn) `Andel av alle besøk`,
  FROM FLAGGET2
 WHERE flagg 
  --  AND Til = 'direkteavspilling'
  --  AND Fra = 'direktesiden'
ORDER BY 1, 4 DESC
