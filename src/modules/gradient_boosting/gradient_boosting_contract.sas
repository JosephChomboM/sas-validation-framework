/* =========================================================================
gradient_boosting_contract.sas - Validaciones de entrada para GB Challenge
========================================================================= */

%include "&fw_root./src/modules/challenge/challenge_contract.sas";

%macro gradient_boosting_contract(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, target=, score_var=, byvar=, id_var=, vars_num=,
    vars_cat=, var_seg=);
    %challenge_contract(input_caslib=&input_caslib., train_table=&train_table.,
        oot_table=&oot_table., target=&target., score_var=&score_var.,
        byvar=&byvar., id_var=&id_var., vars_num=&vars_num.,
        vars_cat=&vars_cat., var_seg=&var_seg.);
%mend gradient_boosting_contract;
