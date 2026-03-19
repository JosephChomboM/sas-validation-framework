/* =========================================================================
steps/methods/metod_9/step_neural_network.sas
Step reservado: Neural Network Challenge (METOD9)
========================================================================= */
%include "&fw_root./src/common/common_public.sas";

%macro _step_neural_network;

    %local _run_neural_network _step_rc _step_status;
    %let _step_rc=0;
    %let _step_status=OK;
    %if %symexist(run_neural_network)=1 %then
        %let _run_neural_network=&run_neural_network.;
    %else %let _run_neural_network=0;

    %fw_log_start(step_name=step_neural_network, run_id=&run_id.,
        fw_root=&fw_root., log_stem=metod_9_step_neural_network);

    %if &_run_neural_network. ne 1 %then %do;
        %put NOTE: [step_neural_network] Modulo deshabilitado
            (run_neural_network=&_run_neural_network.). Saltando.;
        %let _step_status=SKIP;
        %goto _step_neural_network_end;
    %end;

    %put WARNING: [step_neural_network] Estructura reservada para METOD9.
        Neural Network aun no esta implementado en el framework.;
    %let _step_status=SKIP;

%_step_neural_network_end:
    %fw_log_stop(step_name=step_neural_network, step_rc=&_step_rc.,
        step_status=&_step_status);

%mend _step_neural_network;
%_step_neural_network;
