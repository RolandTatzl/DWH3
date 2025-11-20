CREATE   PROCEDURE [etl].[sp_FinishLoadStep]
  (@LoadID int
  ,@StepID int
  ,@LoadStatusID int
  ,@Debug int = 1
  )
as
-- =============================================
-- Author:      solicon-IT, RDTL
-- Create date: 12.11.2025
-- Description: Finishes a DWH load step
-- Change Log:
-- Example: 
-- DECLARE @RetVal int
-- EXEC @RetVal = etl.sp_FinishLoadStep 1711, 10, 2
-- PRINT ('RetVal='+ CONVERT(varchar, @RetVal))
-- 
-- Return Values:
-- 0 Success
-- 1 No running Load
-- 2 Step was not running while trying to set Error-Status or Finish step
-- =============================================
DECLARE
  @ErrorText varchar(4000),
  @LoadTypeID int 

  IF @Debug = 1
    PRINT ('Start Procedure etl.FinishLoadStep');

  -- =============================================
  -- Check if load exists
  -- =============================================  
  SELECT TOP 1 @LoadTypeID = LoadTypeID
    FROM etl.Load
   WHERE LoadID = @LoadID
     and LoadStatusID = 1 -- Running!

  IF @@ROWCOUNT = 0 
  BEGIN
    SET @ErrorText = 'ETL-ERROR: Load Process with LoadID: ' 
                     + CONVERT(varchar, @LoadID) 
                     + ' does not exist or is not running!';
    IF @Debug = 1 
      PRINT (@ErrorText)
    RETURN 1
  END


  -- ==========================================
  -- @LoadStatusID = 2 => step will be finished
  -- ==========================================
  IF @LoadStatusID = 2

  BEGIN
    -- =============================================
    -- Check if load step is running
    -- =============================================  

    SELECT TOP 1 1
      FROM etl.LoadStep
     WHERE LoadID = @LoadID
       AND StepID = @StepID
       AND LoadStatusID = 1

    IF @@ROWCOUNT = 0 
    BEGIN
      SET @ErrorText = 'ETL-ERROR: Step: ' 
                       + CONVERT(varchar, @StepID) 
                       + ' for LoadTypeID: ' 
                       + CONVERT(varchar, @LoadTypeID) 
                       + ' is not runnung and cannot be set to "Finished Successfully"';
      IF @Debug = 1 
        PRINT (@ErrorText)
      RETURN 2
    END

    UPDATE etl.LoadStep
       SET LoadStatusID = 2,
           Message = 'Step completed successfully',
           EndDate = GETDATE()
     WHERE LoadID = @LoadID
       AND StepID = @StepID

    RETURN 0
  END --LoadStatusID=2

  -- ==========================================
  -- @LoadStatusID = 9 => step is errorneous,
  -- finish step and Load Process
  -- ==========================================
  IF @LoadStatusID = 9
  BEGIN
    -- =============================================
    -- Check if load step is running
    -- =============================================  

    SELECT TOP 1 1
      FROM etl.LoadStep
     WHERE LoadID = @LoadID
       AND StepID = @StepID
       AND LoadStatusID = 1

    IF @@ROWCOUNT = 0 
    BEGIN
      SET @ErrorText = 'ETL-ERROR: Step: ' 
                       + CONVERT(varchar, @StepID) 
                       + ' for LoadTypeID: ' 
                       + CONVERT(varchar, @LoadTypeID) 
                       + ' is not runnung and cannot be set to Error-Status';
      IF @Debug = 1 
        PRINT (@ErrorText)
      RETURN 2
    END

    UPDATE etl.LoadStep
       SET LoadStatusID = 9,
           Message = 'Step stopped with error',
           EndDate = GETDATE()
     WHERE LoadID = @LoadID
       AND StepID = @StepID

    UPDATE etl.Load
       SET Message = 'Step: ' + CONVERT(varchar, @StepID) + ' stopped with error',
           EndDate = GETDATE()
     WHERE LoadID = @LoadID

    RETURN 0
  END --LoadStatusID=9
