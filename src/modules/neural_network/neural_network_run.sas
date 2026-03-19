/* =========================================================================
neural_network_run.sas - Placeholder para futuro modulo NN Challenge
========================================================================= */
%include "&fw_root./src/modules/neural_network/neural_network_contract.sas";

%macro neural_network_run(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, output_caslib=OUT, troncal_id=, scope=, run_id=);
    %neural_network_contract();
    %put WARNING: [neural_network_run] Modulo Neural Network reservado para
        METOD9. Aun no esta implementado en el framework.;
%mend neural_network_run;
