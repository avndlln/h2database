CREATE VIEW TEST_VIEW(A) AS SELECT 'a';
> ok

CREATE OR REPLACE VIEW TEST_VIEW(B, C) AS SELECT 'b', 'c';
> ok

SELECT * FROM TEST_VIEW;
> B C
> - -
> b c
> rows: 1
