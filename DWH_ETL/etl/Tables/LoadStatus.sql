CREATE TABLE [etl].[LoadStatus] (
    [LoadStatusID] SMALLINT     NOT NULL,
    [StatusName]   VARCHAR (50) NOT NULL,
    CONSTRAINT [PK_LoadStatus] PRIMARY KEY CLUSTERED ([LoadStatusID] ASC)
);

