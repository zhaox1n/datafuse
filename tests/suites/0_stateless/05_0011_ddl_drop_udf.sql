DROP FUNCTION IF EXISTS isnotempty;

CREATE FUNCTION isnotempty='not(isnull(@0))';
DROP FUNCTION isnotempty;

DROP FUNCTION IF EXISTS isnotempty;

DROP FUNCTION isnotempty; -- {ErrorCode 4071}