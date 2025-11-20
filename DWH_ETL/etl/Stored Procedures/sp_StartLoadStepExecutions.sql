
CREATE   PROCEDURE [etl].[sp_StartLoadStepExecutions] 
  (@pLoadID smallint
  ,@pLoadTypeID smallint
  ,@pStepID  smallint)
AS
-- =============================================
-- Author:      solicon-IT, RDTL
-- Create date: 12.11.2025
-- Description: Start grouped load step executions
-- Change Log:
-- Example: 
-- DECLARE @pLoadID int, @pLoadTypeID int, @pStepID int
-- EXEC etl.sp_StartLoadStepExecutions @pLoadID, @pLoadTypeID, @pStepID
-- =============================================
DECLARE
 @OutStr NVARCHAR(max)
,@vRecExecutionID INT
,@vCode INT
,@vErrm NVARCHAR(3900)
,@vNoDataFound BIT = 0
,@vStatusID INT
,@vMsg NVARCHAR(3900)

BEGIN
  SET NOCOUNT ON;

  BEGIN TRY
    -- Get latest load_id for type 1
    SELECT @pLoadID = MAX(LoadID)
      FROM etl.load
     WHERE LoadTypeID = 1

    -- Cursor simulation using WHILE loop
    DECLARE cur CURSOR FOR
      SELECT ExecutionID, ExecutionString
        FROM etl.LoadTypeStepExecution
       WHERE LoadTypeID = @pLoadTypeID
         AND StepID = @pStepID
       ORDER BY ExecutionID

    OPEN cur
    DECLARE @StepExecutionID INT, @StepExecutionString NVARCHAR(MAX)

    FETCH NEXT FROM cur INTO @StepExecutionID, @StepExecutionString

    WHILE @@FETCH_STATUS = 0
    BEGIN
      SET @vRecExecutionID = @StepExecutionID

      SELECT @vStatusID = LoadStatusID
        FROM etl.LoadStepExecution
       WHERE LoadID = @pLoadID
         AND StepID = @pStepID
         AND StepExecutionID = @vRecExecutionID


      IF @vStatusID IS NULL
      BEGIN
        SET @vNoDataFound = 1
      END

      IF @vNoDataFound = 1
      BEGIN
        INSERT 
          INTO etl.LoadStepExecution
              (
               LoadID
              ,StepID
              ,StepExecutionID
              ,StartDate
              ,LoadStatusID
              ,Message
              ,Command
              )
          VALUES
              (
               @pLoadID
              ,@pStepID
              ,@StepExecutionID
              ,GETDATE()
              ,1
              ,'Step execution started.'
              ,REPLACE(@StepExecutionString, '?', CAST(@pLoadID AS NVARCHAR(20)))
              )
      END

      ELSE IF @vStatusID IN (1, 9)
      BEGIN
        UPDATE etl.LoadStepExecution
           SET StartDate = GETDATE()
              ,LoadStatusID = 1
              ,Message = 'Step execution started.'
         WHERE LoadID = @pLoadID
           AND StepID = @pStepID
           AND StepExecutionID = @StepExecutionID

        COMMIT
      END

      -- Execute dynamic SQL
	    DECLARE @sql NVARCHAR(MAX) = REPLACE(@StepExecutionString, '?', CAST(@pLoadID AS NVARCHAR(20)))

      EXEC sp_executesql @sql

      UPDATE etl.LoadStepExecution
         SET EndDate = GETDATE()
            ,LoadStatusID = 2
            ,Message = 'Step execution successful'
       WHERE LoadID = @pLoadID
         AND StepID = @pStepID
         AND StepExecutionID = @StepExecutionID

      COMMIT

      FETCH NEXT FROM cur INTO @StepExecutionID, @StepExecutionString
    END

    CLOSE cur
    DEALLOCATE cur

  END TRY
  BEGIN CATCH
    SET @vCode = ERROR_NUMBER()
    SET @vErrm = ERROR_MESSAGE()

    UPDATE etl.LoadStepExecution
       SET EndDate = GETDATE()
          ,LoadStatusID = 9
          ,Message = 'Error in Step execution: ' + CAST(@vCode AS NVARCHAR(10)) + ': ' + @vErrm
     WHERE LoadID = @pLoadID
       AND StepID = @pStepID
       AND StepExecutionID = @StepExecutionID

    COMMIT

	  SET @vMsg = 'Error in Step execution: ' + CAST(@vCode AS NVARCHAR(10)) + ': ' + @vErrm;

    THROW 50001, @vMsg, 1
  END CATCH
END
