CREATE   PROCEDURE [etl].[sp_StartLoad]
  (@LoadTypeID smallint
  ,@Debug int = 0
  ,@LoadID int OUTPUT
  ,@StartDate datetime2(2) OUTPUT
  ,@isFullLoad tinyint OUTPUT)
AS
-- =============================================
-- Author:      solicon-IT, RDTL
-- Create date: 12.11.2025
-- Description: (Re-)Start a DWH load process 
-- Change Log:
-- Example: 
-- DECLARE @LoadID int, @StartDate datetime2(2), @isFullLoad tinyint;
-- EXEC etl.sp_StartLoad  1, 1, @LoadID OUTPUT, @StartDate OUTPUT, @isFullLoad OUTPUT
-- PRINT ('LoadID='+ CONVERT(varchar, @LoadID))
-- PRINT ('StartDate=' + CONVERT(varchar, @StartDate, 112))
-- PRINT ('isFullLoad=' + CONVERT(varchar, @isFullLoad))
-- =============================================
DECLARE
  @ErrorText varchar(4000)

  IF @Debug = 1
    PRINT ('Start Procedure ETL.StartLoad');

  -- =============================================
  -- Check if load is running
  -- =============================================  
  SELECT TOP 1 @LoadID = LoadID
    FROM etl.Load
   WHERE LoadStatusID = 1 -- Running
     AND LoadTypeID = @LoadTypeID 

  IF @@ROWCOUNT > 0 
  BEGIN
    SET @ErrorText = 'ETL-ERROR: Load cannot be started because LoadID: ' +
                     CONVERT(varchar, @LoadID) + ' is still running';   
    THROW 51000, @ErrorText, 1;  
  END

  IF @Debug = 1
  BEGIN
    PRINT ('End check for running load');
    PRINT ('Start get LoadID')
  END

  -- =============================================
  -- Generate and return new Load_ID 
  -- =============================================
  IF @Debug = 1 
    PRINT ('Start generate and return LoadID ')

  -- =============================================
  -- Check if errorneous load exists for restart
  -- =============================================

  SELECT TOP 1 @LoadID = LoadID
    FROM etl.Load
   WHERE LoadStatusID = 9 -- Error
     AND LoadTypeID = @LoadTypeID 

  IF @@ROWCOUNT > 0 
    UPDATE etl.Load
     SET Message = 'Load ' + CONVERT(varchar, @LoadID) + ' was re-started',
         LoadStatusID = 1
   WHERE LoadID = @LoadID
  ELSE
  BEGIN
    SELECT TOP 1 @LoadID = coalesce(max(LoadID)+1,1), @StartDate = GETDATE()
    FROM etl.Load

    SELECT @isFullLoad = isFullLoad 
      FROM etl.LoadType
     WHERE LoadTypeID = @LoadTypeID
    IF @@ROWCOUNT = 0 
    BEGIN
      SET @ErrorText = 'ETL-ERROR: LoadTypeID: ' + CONVERT(varchar, @LoadTypeID) + ' is unknown';   
      THROW 51000, @ErrorText, 1;  
    END

    INSERT INTO [etl].[Load]
           ([LoadID]
           ,[LoadTypeID]
           ,[LoadStatusID]
           ,[StartDate]
           ,[EndDate]
           ,[isFullLoad]
           ,[Message])
    VALUES
           (@LoadID
           ,@LoadTypeID
           ,1
           ,@StartDate
           ,null
           ,@isFullLoad
           ,'Load '+ CONVERT(varchar, @LoadID) + ' was started'
       )
  END
  IF @Debug = 1 
  BEGIN
    PRINT ('End generate and return new LoadID: ' + CONVERT(varchar, @LoadID));
    PRINT ('End Procedure etl.StartLoad');
  END
