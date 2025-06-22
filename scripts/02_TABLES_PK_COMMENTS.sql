USE proyecto02;

-- Eliminar tablas en orden correcto (respetando claves foráneas)
DROP TABLE IF EXISTS TopWordPages;
DROP TABLE IF EXISTS Top2WordsPages;
DROP TABLE IF EXISTS Top3WordsPages;
DROP TABLE IF EXISTS PageXWord;
DROP TABLE IF EXISTS Sets2PageXPage;
DROP TABLE IF EXISTS Sets3PageXPage;
DROP TABLE IF EXISTS SetWords2;
DROP TABLE IF EXISTS SetWords3;
DROP TABLE IF EXISTS Word;
DROP TABLE IF EXISTS Page;

-- Crear tabla Page con todas las columnas necesarias
CREATE TABLE Page (
    id_page INT AUTO_INCREMENT COMMENT 'Unique identifier of the page.',
    title VARCHAR(500) NOT NULL COMMENT 'Title of the page.',
    url VARCHAR(1000) NOT NULL COMMENT 'URL of the page.',
    edits_per_day DOUBLE DEFAULT 0.0 COMMENT 'Average edits per day.',
    quant_diff_urls INT DEFAULT 0 COMMENT 'Quantity of different URLs linked.',
    quant_set2 INT DEFAULT 0 COMMENT 'Quantity of word pairs.',
    quant_set3 INT DEFAULT 0 COMMENT 'Quantity of word triplets.',
    total_repetitions INT DEFAULT 1 COMMENT 'Total repetitions.',
    PRIMARY KEY (id_page)
);

-- Crear tabla Word con columnas necesarias
CREATE TABLE Word (
    id_word INT AUTO_INCREMENT COMMENT 'Unique identifier of the word.',
    word VARCHAR(255) NOT NULL COMMENT 'Name of the word.',
    total_repetitions INT DEFAULT 0 COMMENT 'Total repetitions across all pages.',
    PRIMARY KEY (id_word),
    UNIQUE KEY unique_word (word)
);

-- Crear tabla SetWords2 con columnas necesarias
CREATE TABLE SetWords2 (
    id_set2 INT AUTO_INCREMENT COMMENT 'Unique identifier of the set of 2 words.',
    word1 VARCHAR(255) NOT NULL COMMENT 'First word of the pair.',
    word2 VARCHAR(255) NOT NULL COMMENT 'Second word of the pair.',
    PRIMARY KEY (id_set2),
    UNIQUE KEY unique_pair (word1, word2)
);

-- Crear tabla SetWords3 con columnas necesarias
CREATE TABLE SetWords3 (
    id_set3 INT AUTO_INCREMENT COMMENT 'Unique identifier of the set of 3 words.',
    word1 VARCHAR(255) NOT NULL COMMENT 'First word of the triplet.',
    word2 VARCHAR(255) NOT NULL COMMENT 'Second word of the triplet.',
    word3 VARCHAR(255) NOT NULL COMMENT 'Third word of the triplet.',
    PRIMARY KEY (id_set3),
    UNIQUE KEY unique_triplet (word1, word2, word3)
);

-- Crear tabla TopWordPages (relación entre Word y Page)
CREATE TABLE TopWordPages (
    id_top INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_word INT NOT NULL COMMENT 'Foreign key to Word table.',
    id_page INT NOT NULL COMMENT 'Foreign key to Page table.',
    quantity INT NOT NULL COMMENT 'Frequency of word in page.',
    PRIMARY KEY (id_top),
    FOREIGN KEY (id_word) REFERENCES Word(id_word) ON DELETE CASCADE,
    FOREIGN KEY (id_page) REFERENCES Page(id_page) ON DELETE CASCADE,
    UNIQUE KEY unique_word_page (id_word, id_page)
);

-- Crear tabla Top2WordsPages (relación entre SetWords2 y Page)
CREATE TABLE Top2WordsPages (
    id_top2 INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_set2 INT NOT NULL COMMENT 'Foreign key to SetWords2 table.',
    id_page INT NOT NULL COMMENT 'Foreign key to Page table.',
    repetition_count INT NOT NULL COMMENT 'Frequency of word pair in page.',
    PRIMARY KEY (id_top2),
    FOREIGN KEY (id_set2) REFERENCES SetWords2(id_set2) ON DELETE CASCADE,
    FOREIGN KEY (id_page) REFERENCES Page(id_page) ON DELETE CASCADE,
    UNIQUE KEY unique_set2_page (id_set2, id_page)
);

-- Crear tabla Top3WordsPages (relación entre SetWords3 y Page)
CREATE TABLE Top3WordsPages (
    id_top3 INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_set3 INT NOT NULL COMMENT 'Foreign key to SetWords3 table.',
    id_page INT NOT NULL COMMENT 'Foreign key to Page table.',
    repetition_count INT NOT NULL COMMENT 'Frequency of word triplet in page.',
    PRIMARY KEY (id_top3),
    FOREIGN KEY (id_set3) REFERENCES SetWords3(id_set3) ON DELETE CASCADE,
    FOREIGN KEY (id_page) REFERENCES Page(id_page) ON DELETE CASCADE,
    UNIQUE KEY unique_set3_page (id_set3, id_page)
);

-- Crear tabla PageXWord (si es necesaria para otras relaciones)

CREATE TABLE PageXWord (
    id_pageXword INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_page INT NOT NULL COMMENT 'Foreign key to Page table.',
    id_word INT NOT NULL COMMENT 'Foreign key to Word table.',
    PRIMARY KEY (id_pageXword),
    FOREIGN KEY (id_page) REFERENCES Page(id_page) ON DELETE CASCADE,
    FOREIGN KEY (id_word) REFERENCES Word(id_word) ON DELETE CASCADE
);

-- Crear tabla Sets2PageXPage (si es necesaria)
CREATE TABLE Sets2PageXPage (
    id_sets2_pxp INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_page1 INT NOT NULL COMMENT 'Foreign key to first Page.',
    id_page2 INT NOT NULL COMMENT 'Foreign key to second Page.',
    id_set2 INT NOT NULL COMMENT 'Foreign key to SetWords2.',
    PRIMARY KEY (id_sets2_pxp),
    FOREIGN KEY (id_page1) REFERENCES Page(id_page) ON DELETE CASCADE,
    FOREIGN KEY (id_page2) REFERENCES Page(id_page) ON DELETE CASCADE,
    FOREIGN KEY (id_set2) REFERENCES SetWords2(id_set2) ON DELETE CASCADE
);

-- Crear tabla Sets3PageXPage (si es necesaria)
CREATE TABLE Sets3PageXPage (
    id_sets3_pxp INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_page1 INT NOT NULL COMMENT 'Foreign key to first Page.',
    id_page2 INT NOT NULL COMMENT 'Foreign key to second Page.',
    id_set3 INT NOT NULL COMMENT 'Foreign key to SetWords3.',
    PRIMARY KEY (id_sets3_pxp),
    FOREIGN KEY (id_page1) REFERENCES Page(id_page) ON DELETE CASCADE,
    FOREIGN KEY (id_page2) REFERENCES Page(id_page) ON DELETE CASCADE,
    FOREIGN KEY (id_set3) REFERENCES SetWords3(id_set3) ON DELETE CASCADE
);