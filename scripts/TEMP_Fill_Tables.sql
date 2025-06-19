USE proyecto02;

-- Páginas
INSERT INTO Page (title, url, edits_per_day, quant_diff_urls, quant_set2, quant_set3, total_repetitions)
VALUES 
('Inteligencia Artificial', 'https://es.wikipedia.org/wiki/IA', 5, 8, 10, 7, 200),
('Aprendizaje Automático', 'https://es.wikipedia.org/wiki/ML', 3, 5, 8, 4, 150);

-- Palabras
INSERT INTO Word (word, total_repetitions)
VALUES 
('inteligencia', 120),
('artificial', 100),
('aprendizaje', 90),
('automático', 80);

-- PageXWord
INSERT INTO PageXWord (id_page, id_word, percentage, quantity)
VALUES 
(1, 1, 0.15, 30), -- "inteligencia" en página 1
(1, 2, 0.10, 20), -- "artificial" en página 1
(2, 3, 0.20, 30), -- "aprendizaje" en página 2
(2, 4, 0.10, 15); -- "automático" en página 2

-- TopWordPages
INSERT INTO TopWordPages (id_word, id_page, quantity)
VALUES 
(1, 1, 30),
(3, 2, 30);

-- Bigrams
INSERT INTO SetWords2 (word1, word2) VALUES 
('inteligencia', 'artificial'),
('aprendizaje', 'automático');

-- Top2WordsPages
INSERT INTO Top2WordsPages (id_set2, id_page, repetition_count)
VALUES 
(1, 1, 18),
(2, 2, 15);

-- Trigrams
INSERT INTO SetWords3 (word1, word2, word3) VALUES 
('redes', 'neuronales', 'artificiales'),
('aprendizaje', 'no', 'supervisado');

-- Top3WordsPages
INSERT INTO Top3WordsPages (id_set3, id_page, repetition_count)
VALUES 
(1, 1, 10),
(2, 2, 12);

-- Coincidencias entre páginas
INSERT INTO Sets2PageXPage (id_page1, id_page2, shared_sets_count)
VALUES (1, 2, 3);

INSERT INTO Sets3PageXPage (id_page1, id_page2, shared_sets_count)
VALUES (1, 2, 2);
