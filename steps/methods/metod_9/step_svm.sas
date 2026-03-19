/* =========================================================================
steps/methods/metod_9/step_svm.sas
Step reservado: Support Vector Machine Challenge (METOD9)
========================================================================= */
%include "&fw_root./src/common/common_public.sas";

%macro _step_svm;

    %local _run_svm _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;
    %if %symexist(run_svm)=1 %then %let _run_svm=&run_svm.;
    %else %let _run_svm=0;

    %fw_log_start(step_name=step_svm, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_9_step_svm);

    %if &_run_svm. ne 1 %then %do;
        %put NOTE: [step_svm] Modulo deshabilitado (run_svm=&_run_svm.).
            Saltando.;
        %let _step_status=SKIP;
        %goto _step_svm_end;
    %end;

    %put WARNING: [step_svm] Estructura reservada para METOD9.
        Support Vector Machine aun no esta implementado en el framework.;
    %let _step_status=SKIP;

%_step_svm_end:
    %fw_log_stop(step_name=step_svm, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_svm;
%_step_svm;
