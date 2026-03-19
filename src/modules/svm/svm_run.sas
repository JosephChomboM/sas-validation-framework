/* =========================================================================
svm_run.sas - Placeholder para futuro modulo SVM Challenge
========================================================================= */
%include "&fw_root./src/modules/svm/svm_contract.sas";

%macro svm_run(input_caslib=PROC, train_table=_train_input, oot_table=_oot_input,
    output_caslib=OUT, troncal_id=, scope=, run_id=);
    %svm_contract();
    %put WARNING: [svm_run] Modulo SVM reservado para METOD9.
        Aun no esta implementado en el framework.;
%mend svm_run;
