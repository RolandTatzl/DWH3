CREATE   PROCEDURE [etl].[sp_StartLoadStep]
  (@LoadID int
  ,@StepID int
  ,@LoadStatusID int = 1
  ,@Debug int = 1
  ,@logg_LoadID int = NULL
  )
as
-- =============================================
-- Author:      solicon-IT, RDTL
-- Create date: 12.11.2025
-- Description: Starts a DWH load step
-- Change Log:
-- Example: 
-- DECLARE @RetVal int
-- EXEC @RetVal = etl.sp_StartLoadStep 1711, 10, 1
-- PRINT ('RetVal='+ CONVERT(varchar, @RetVal))
-- 
-- Return Values:
-- 0 Success
-- 1 No running Load
-- 2 Step cannot be started becaus it is not active
-- 3 Step cannot be started because it was is already finished successfully (Message)
-- 4 Step cannot be started because it is already running
-- =============================================
DECLARE
  @ErrorText varchar(4000),
  @LoadTypeID int 

  IF @Debug = 1
    PRINT ('Start Procedure etl.StartLoad');

  -- =============================================
  -- Check if load exists
  -- =============================================  
  SELECT TOP 1 @LoadTypeID = LoadTypeID
    FROM etl.Load
   WHERE LoadID = @LoadID
     and LoadStatusID = 1 -- Running!

  IF @@ROWCOUNT = 0 
  BEGIN
    SET @ErrorText = 'ETL-ERROR: Load with LoadID: ' 
                     + CONVERT(varchar, @LoadID) 
                     + ' does not exist or is not running!';
    IF @Debug = 1 
      PRINT (@ErrorText)
    RETURN 1
  END

  -- =============================================
  -- @LoadStatusID = 1 => step will be started
  -- =============================================

  IF @LoadStatusID = 1
  BEGIN
    -- =============================================
    -- Check if load step is inactive
    -- =============================================  
    SELECT 1
      FROM etl.LoadTypeStep
     WHERE LoadTypeID = @LoadTypeID
       AND StepID = @StepID
       AND isActive = 0

    IF @@ROWCOUNT > 0 
    BEGIN
      SET @ErrorText = 'ETL-ERROR: Step: ' 
                       + CONVERT(varchar, @StepID) 
                       + ' for LoadTypeID: ' 
                       + CONVERT(varchar, @LoadTypeID) 
                       + ' cannot be started because step is not active';
      IF @Debug = 1 
        PRINT (@ErrorText)
      RETURN 2
    END

    -- =============================================
    -- Check if load step is finished
    -- =============================================  
    SELECT 1
      FROM etl.LoadStep
     WHERE LoadID = @LoadID
       AND StepID = @StepID
       AND LoadStatusID = 2

    IF @@ROWCOUNT > 0 
    BEGIN
      SET @ErrorText = 'ETL-MESSAGE: Step: ' 
                       + CONVERT(varchar, @StepID) 
                       + ' for LoadTypeID: ' 
                       + CONVERT(varchar, @LoadTypeID) 
                       + ' has already been finished successfully';
      IF @Debug = 1 
        PRINT (@ErrorText)
      RETURN 3
    END

    SELECT 1
      FROM etl.LoadStep
     WHERE LoadID = @LoadID
       AND StepID = @StepID
       AND LoadStatusID = 1

    IF @@ROWCOUNT > 0 
    BEGIN
      SET @ErrorText = 'ETL-ERROR: Step: ' 
                       + CONVERT(varchar, @StepID) 
                       + ' for LoadTypeID: ' 
                       + CONVERT(varchar, @LoadTypeID) 
                       + ' cannot be started as it is currently running';
      IF @Debug = 1 
        PRINT (@ErrorText)
      RETURN 4
    END

    -- ================================================
    -- Set existing LoadStep to Status "running"
    -- ================================================
    UPDATE etl.LoadStep
       SET LoadStatusID = 1
          ,Message = 'Running...'
          ,EndDate = Null
          ,logg_LoadID = @logg_LoadID
     WHERE LoadID = @LoadID
       AND StepID = @StepID

    IF @@ROWCOUNT = 0 
    -- ==========================
    -- Insert new LoadStep
    -- ==========================
      INSERT INTO [etl].[LoadStep]
           ([LoadID]
           ,[StepID]
           ,[StartDate]
           ,[EndDate]
           ,[LoadStatusID]
           ,[Message]
           ,[logg_LoadID]
           )
      VALUES
           (@LoadID
           ,@StepID
           ,GETDATE()
           ,null
           ,1
           ,'Running...'
           ,@logg_loadID
           )  

    UPDATE etl.Load
       SET Message = 'Running Step ' + CONVERT(varchar, @StepID),
           EndDate = Null
     WHERE LoadID = @LoadID

    RETURN 0

  END -- LoadStatusID=1


-- END PROCEDURE --
