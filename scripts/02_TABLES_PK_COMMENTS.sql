USE proyecto02;

CREATE TABLE Word (
    id_word INT AUTO_INCREMENT COMMENT 'Unique identifier of the word.',
    word VARCHAR(255) NOT NULL COMMENT 'Name of the word.',
    total_repetitions INT NOT NULL COMMENT 'Word repetions in all pages.',
    PRIMARY KEY (id_word)
);

CREATE TABLE Page (
    id_page INT AUTO_INCREMENT COMMENT 'Unique identifier of the page.',
    title VARCHAR(255) NOT NULL COMMENT 'Name of the page.',
    url VARCHAR(255) NOT NULL COMMENT 'Link of the page.',
    edits_per_day INT NOT NULL COMMENT 'Quantity of editions in a day.',
    quant_diff_urls INT NOT NULL COMMENT 'Quantity of different links in a page',
    quant_set2 INT NOT NULL COMMENT 'Quantity of different sets of 2 words',
    quant_set3 INT NOT NULL COMMENT 'Quantity of different sets of 3 words',
    total_repetitions INT NOT NULL COMMENT 'Page repetions in all pages.',
    PRIMARY KEY (id_page)
);

CREATE TABLE TopWordPages (
    id_top INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_word INT NOT NULL COMMENT 'Foreign key as identifier of the word.',
    id_page INT NOT NULL COMMENT 'Foreign key as identifier of the page.',
    quantity INT NOT NULL COMMENT 'Total of repetions in this page',
    FOREIGN KEY (id_word) REFERENCES Word(id_word),
    FOREIGN KEY (id_page) REFERENCES Page(id_page),
    PRIMARY KEY (id_top)
);

CREATE TABLE SetWords2 (
    id_set2 INT AUTO_INCREMENT COMMENT 'Unique identifier of the set of 2 words.',
    word1 VARCHAR(255) NOT NULL COMMENT 'Name of the first word.',
    word2 VARCHAR(255) NOT NULL COMMENT 'Name of the second word.',
    UNIQUE(word1, word2),
    PRIMARY KEY (id_set2)
);

CREATE TABLE Top2WordsPages (
    id_top2 INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_set2 INT NOT NULL COMMENT 'Foreign key as identifier of the set of 2 words.',
    id_page INT NOT NULL COMMENT 'Foreign key as identifier of the page.',
    repetition_count INT NOT NULL COMMENT 'Quantity of repetions for the set in this page',
    FOREIGN KEY (id_set2) REFERENCES SetWords2(id_set2),
    FOREIGN KEY (id_page) REFERENCES Page(id_page),
    PRIMARY KEY (id_top2)
);

CREATE TABLE SetWords3 (
    id_set3 INT AUTO_INCREMENT COMMENT 'Unique identifier of the set of 3 words.',
    word1 VARCHAR(255) NOT NULL COMMENT 'Name of the first word.',
    word2 VARCHAR(255) NOT NULL COMMENT 'Name of the second word.',
    word3 VARCHAR(255) NOT NULL COMMENT 'Name of the third word.',
    UNIQUE(word1, word2, word3),
    PRIMARY KEY (id_set3)
);

CREATE TABLE Top3WordsPages (
    id_top3 INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_set3 INT NOT NULL COMMENT 'Foreign key as identifier of the set of 3 words.',
    id_page INT NOT NULL COMMENT 'Foreign key as identifier of the page.',
    repetition_count INT NOT NULL COMMENT 'Quantity of repetions for the set in this page',
    FOREIGN KEY (id_set3) REFERENCES SetWords3(id_set3),
    FOREIGN KEY (id_page) REFERENCES Page(id_page),
    PRIMARY KEY (id_top3)
);

CREATE TABLE PageXWord (
    id_pageXword INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_page INT NOT NULL COMMENT 'Foreign key as identifier of the page.',
    id_word INT NOT NULL COMMENT 'Foreign key as identifier of the word.',
    percentage FLOAT NOT NULL COMMENT 'Percentage of repetitions in this page',
    quantity INT NOT NULL COMMENT 'Total of repetitions in this page',
    FOREIGN KEY (id_word) REFERENCES Word(id_word),
    FOREIGN KEY (id_page) REFERENCES Page(id_page),
    PRIMARY KEY (id_pageXword)
);

CREATE TABLE Sets2PageXPage (
    id_sets2_pxp INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_page1 INT NOT NULL COMMENT 'Foreign key as identifier of the first page.',
    id_page2 INT NOT NULL COMMENT 'Foreign key as identifier of the second page.',
    shared_sets_count INT NOT NULL COMMENT 'Quantity of sets shared',
    PRIMARY KEY (id_sets2_pxp)
);

CREATE TABLE Sets3PageXPage (
    id_sets3_pxp INT AUTO_INCREMENT COMMENT 'Unique identifier of the relation.',
    id_page1 INT NOT NULL COMMENT 'Foreign key as identifier of the first page.',
    id_page2 INT NOT NULL COMMENT 'Foreign key as identifier of the second page.',
    shared_sets_count INT NOT NULL COMMENT 'Quantity of sets shared',
    PRIMARY KEY (id_sets3_pxp)
);
