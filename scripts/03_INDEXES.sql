USE proyecto02;

-- Indices for word's queries

CREATE UNIQUE INDEX idx_word_unique ON Word(word);

-- Indices for page's queries

CREATE UNIQUE INDEX idx_url_unique ON Page(url(255));

CREATE INDEX idx_title ON Page(title);

-- Indices for TopWordPages's queries

CREATE INDEX idx_twp_word ON TopWordPages(id_word);
CREATE INDEX idx_twp_page ON TopWordPages(id_page);

-- Indices for Top2WordsPages's queries

CREATE INDEX idx_t2wp_set ON Top2WordsPages(id_set2);
CREATE INDEX idx_t2wp_page ON Top2WordsPages(id_page);

-- Indices for Top3WordsPages's queries

CREATE INDEX idx_t3wp_set ON Top3WordsPages(id_set3);
CREATE INDEX idx_t3wp_page ON Top3WordsPages(id_page);

-- Indices for PageXWord's queries

CREATE INDEX idx_pxw_page ON PageXWord(id_page);
CREATE INDEX idx_pxw_word ON PageXWord(id_word);

-- Indices for Sets2PageXPage's queries

CREATE INDEX idx_s2pxp_page1 ON Sets2PageXPage(id_page1);
CREATE INDEX idx_s2pxp_page2 ON Sets2PageXPage(id_page2);

-- Indices for Sets3PageXPage's queries

CREATE INDEX idx_s3pxp_page1 ON Sets3PageXPage(id_page1);
CREATE INDEX idx_s3pxp_page2 ON Sets3PageXPage(id_page2);



