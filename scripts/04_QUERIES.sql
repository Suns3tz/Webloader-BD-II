USE proyecto02;

DELIMITER $$

-- 1. Para cada palabra distinta, ¿Cuáles son las páginas que más copias de esta palabra tienen?

CREATE FUNCTION getTopPagesByWord(pvWord VARCHAR(255))
RETURNS JSON
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result JSON;
	
    -- Check that the parameter is not null
    IF pvWord IS NULL THEN
        RETURN JSON_OBJECT('error', 'Word parameter cannot be NULL');
    END IF;

    SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'TITLE', p.title,
        'URL', p.url,
        'QUANTITY', pxw.quantity
    ))
    INTO result
    FROM Page p
    JOIN PageXWord pxw ON pxw.id_page = p.id_page
    JOIN Word w ON pxw.id_word = w.id_word
    WHERE LOWER(w.word) = LOWER(pvWord)
    ORDER BY pxw.quantity DESC;
    
    RETURN result;
END $$

-- 2. Para cada palabra set de 2 palabras, ¿Cuáles son las páginas que más copias de este set de
-- palabras tiene?

CREATE FUNCTION getTopPagesBySet2(pvWord1 VARCHAR(255), pvWord2 VARCHAR(255))
RETURNS JSON
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result JSON;
	
    -- Check that the parameters are not null
    IF pvWord1 IS NULL OR pvWord2 IS NULL THEN
        RETURN JSON_OBJECT('error', 'Set of two parameters cannot be NULL');
    END IF;

    SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'TITLE', p.title,
        'URL', p.url,
        'REPETITION_COUNT', twp.repetition_count
    ))
    INTO result
    FROM Page p
    JOIN Top2WordsPages twp ON twp.id_page = p.id_page
    JOIN SetWords2 sw ON twp.id_set2 = sw.id_set2
    WHERE LOWER(sw.word1) = LOWER(pvWord1) AND LOWER(sw.word2) = LOWER(pvWord2)
    ORDER BY twp.repetition_count DESC;
    
    RETURN result;
END $$

-- 3. Para cada palabra set de 3 palabras, ¿Cuáles son las páginas que más copias de este set de
-- palabras tiene?

CREATE FUNCTION getTopPagesBySet3(pvWord1 VARCHAR(255), pvWord2 VARCHAR(255), pvWord3 VARCHAR(255))
RETURNS JSON
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result JSON;
	
    -- Check that the parameters are not null
    IF pvWord1 IS NULL OR pvWord2 IS NULL OR pvWord3 IS NULL THEN
        RETURN JSON_OBJECT('error', 'Set of three parameters cannot be NULL');
    END IF;

    SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'TITLE', p.title,
        'URL', p.url,
        'REPETITION_COUNT', twp.repetition_count
    ))
    INTO result
    FROM Page p
    JOIN Top3WordsPages twp ON twp.id_page = p.id_page
    JOIN SetWords3 sw ON twp.id_set3 = sw.id_set3
    WHERE LOWER(sw.word1) = LOWER(pvWord1) AND LOWER(sw.word2) = LOWER(pvWord2)
    AND LOWER(sw.word3) = LOWER(pvWord3)
    ORDER BY twp.repetition_count DESC;
    
    RETURN result;
END $$

-- 4. ¿Cuáles páginas tienen más sets de 2 palabras coincidentes con una página dada?

CREATE FUNCTION getSharedBigramsByPage(pvUrl VARCHAR(255))
RETURNS JSON
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result JSON;
    DECLARE vPageId INT;
	
    -- Check that the parameter is not null
    IF pvUrl IS NULL THEN
        RETURN JSON_OBJECT('error', 'URL cannot be NULL');
    END IF;

    -- Get the id_page assigned to the URL
    SELECT id_page INTO vPageId
    FROM Page
    WHERE url = pvUrl
    LIMIT 1;
    
    -- Check that the id_page is not null
    IF vPageId IS NULL THEN
        RETURN JSON_OBJECT('error', 'No page found for given URL');
    END IF;
    
    SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'TITLE', p.title,
        'URL', p.url,
        'SHARED_SETS', spxp.shared_sets_count
    ))
    INTO result
    FROM Sets2PageXPage spxp
    JOIN Page p ON p.id_page = spxp.id_page2
    WHERE spxp.id_page1 = vPageId
    ORDER BY spxp.shared_sets_count DESC;

    RETURN result;
END $$

-- 5. ¿Cuáles páginas tienen más sets de 3 palabras coincidentes con una página dada?

CREATE FUNCTION getSharedTrigramsByPage(pvUrl VARCHAR(255))
RETURNS JSON
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result JSON;
    DECLARE vPageId INT;
	
    -- Check that the parameter is not null
    IF pvUrl IS NULL THEN
        RETURN JSON_OBJECT('error', 'URL cannot be NULL');
    END IF;

    -- Get the id_page assigned to the URL
    SELECT id_page INTO vPageId
    FROM Page
    WHERE url = pvUrl
    LIMIT 1;
    
    -- Check that the id_page is not null
    IF vPageId IS NULL THEN
        RETURN JSON_OBJECT('error', 'No page found for given URL');
    END IF;
    
    SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'TITLE', p.title,
        'URL', p.url,
        'SHARED_SETS', spxp.shared_sets_count
    ))
    INTO result
    FROM Sets3PageXPage spxp
    JOIN Page p ON p.id_page = spxp.id_page2
    WHERE spxp.id_page1 = vPageId
    ORDER BY spxp.shared_sets_count DESC;

    RETURN result;
END $$

-- 6. Para cada página, ¿Cuál es el set de palabras distintas y cuantas hay de cada una?

CREATE FUNCTION getDifferentWordsByPage(pvUrl VARCHAR(255))
RETURNS JSON
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result JSON;
    DECLARE vPageId INT;
	
    -- Check that the parameter is not null
    IF pvUrl IS NULL THEN
        RETURN JSON_OBJECT('error', 'URL cannot be NULL');
    END IF;

    -- Get the id_page assigned to the URL
    SELECT id_page INTO vPageId
    FROM Page
    WHERE url = pvUrl
    LIMIT 1;
    
    -- Check that the id_page is not null
    IF vPageId IS NULL THEN
        RETURN JSON_OBJECT('error', 'No page found for given URL');
    END IF;

    SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'WORD', w.word,
        'QUANTITY', pxw.quantity
    ))
    INTO result
    FROM PageXWord pxw
    JOIN Word w ON pxw.id_word = w.id_word
    WHERE pxw.id_page = vPageId
    ORDER BY pxw.quantity DESC;

    RETURN result;
END $$

-- 7. Para cada página ¿cuántos links distintos aparecen en cada página?

CREATE FUNCTION getLinkCountByPage(pvUrl VARCHAR(255))
RETURNS JSON
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result JSON;
    DECLARE vPageId INT;
	
    -- Check that the parameter is not null
    IF pvUrl IS NULL THEN
        RETURN JSON_OBJECT('error', 'URL cannot be NULL');
    END IF;

    -- Get the id_page assigned to the URL
    SELECT id_page INTO vPageId
    FROM Page
    WHERE url = pvUrl
    LIMIT 1;
    
    -- Check that the id_page is not null
    IF vPageId IS NULL THEN
        RETURN JSON_OBJECT('error', 'No page found for given URL');
    END IF;

    SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'TITLE', title,
        'URL', url,
        'QUANTITY_URLS', quant_diff_urls
    ))
    INTO result
    FROM Page
    WHERE id_page = vPageId;

    RETURN result;
END $$

-- 8. ¿Cuál es el porcentaje que cada palabra distinta en el texto total de la página?

CREATE FUNCTION getPercentageWordsByPage(pvUrl VARCHAR(255))
RETURNS JSON
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result JSON;
    DECLARE vPageId INT;
	
    -- Check that the parameter is not null
    IF pvUrl IS NULL THEN
        RETURN JSON_OBJECT('error', 'URL cannot be NULL');
    END IF;

    -- Get the id_page assigned to the URL
    SELECT id_page INTO vPageId
    FROM Page
    WHERE url = pvUrl
    LIMIT 1;
    
    -- Check that the id_page is not null
    IF vPageId IS NULL THEN
        RETURN JSON_OBJECT('error', 'No page found for given URL');
    END IF;

    SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'WORD', w.word,
        'PERCENTAGE', pxw.percentage
    ))
    INTO result
    FROM PageXWord pxw
    JOIN Word w ON pxw.id_word = w.id_word
    WHERE pxw.id_page = vPageId
    ORDER BY pxw.percentage DESC;

    RETURN result;
END $$

-- 9. Hacer un grafo con cómo los enlaces se conectan con otras páginas, de esta manera pueden
-- tener los resultados de cuáles son los tópicos que más interconectados están con otras páginas.


-- Agregar 2 análisis distintos que ustedes crean pertinente.

-- 10. La cantidad de repeticiones de una palabra en el total de la informacion

CREATE FUNCTION getTotalRepetitionsByWord(pvWord VARCHAR(255))
RETURNS JSON
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result JSON;
	
    -- Check that the parameter is not null
    IF pvWord IS NULL THEN
        RETURN JSON_OBJECT('error', 'Word parameter cannot be NULL');
    END IF;

    SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'WORD', word,
        'TOTAL_REPETITIONS', total_repetitions
    ))
    INTO result
    FROM Word 
    WHERE LOWER(word) = LOWER(pvWord);
    
    RETURN result;
END $$

-- 11. La cantidad de repeticiones de una pagina en el total de la informacion

CREATE FUNCTION getTotalRepetitionsByPage(pvUrl VARCHAR(255))
RETURNS JSON
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE result JSON;
    DECLARE vPageId INT;
	
    -- Check that the parameter is not null
    IF pvUrl IS NULL THEN
        RETURN JSON_OBJECT('error', 'URL cannot be NULL');
    END IF;

    -- Get the id_page assigned to the URL
    SELECT id_page INTO vPageId
    FROM Page
    WHERE url = pvUrl
    LIMIT 1;
    
    -- Check that the id_page is not null
    IF vPageId IS NULL THEN
        RETURN JSON_OBJECT('error', 'No page found for given URL');
    END IF;

    SELECT JSON_ARRAYAGG(JSON_OBJECT(
        'TITLE', title,
        'URL', url,
        'EDITS_PER_DAY', edits_per_day,
        'TOTAL_REPETITIONS', total_repetitions
    ))
    INTO result
    FROM Page
    WHERE id_page = vPageId;

    RETURN result;
END $$

