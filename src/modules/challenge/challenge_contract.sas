/* =========================================================================
challenge_contract.sas - Validaciones de entrada para METOD9 Challenge
========================================================================= */

%macro challenge_contract(input_caslib=PROC, train_table=_train_input,
    oot_table=_oot_input, target=, score_var=, byvar=, id_var=, vars_num=,
    vars_cat=, var_seg=);

    %global _chall_rc;
    %local _chall_nobs_trn _chall_nobs_oot _chall_has_col;
    %let _chall_rc=0;

    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [challenge_contract] No hay variables num/cat validas para
            entrenar el challenge.;
        %let _chall_rc=1;
        %return;
    %end;

    %if %length(%superq(target))=0 or %length(%superq(score_var))=0 or
        %length(%superq(byvar))=0 %then %do;
        %put ERROR: [challenge_contract] target, score_var y byvar son
            obligatorios.;
        %let _chall_rc=1;
        %return;
    %end;

    proc sql noprint;
        select count(*) into :_chall_nobs_trn trimmed
        from &input_caslib..&train_table.;
        select count(*) into :_chall_nobs_oot trimmed
        from &input_caslib..&oot_table.;
    quit;

    %if &_chall_nobs_trn.=0 or &_chall_nobs_oot.=0 %then %do;
        %put ERROR: [challenge_contract] TRAIN u OOT vacios o no accesibles.;
        %let _chall_rc=1;
        %return;
    %end;

    %macro _chall_chk(mem=, col=, req=1);
        %let _chall_has_col=0;
        proc sql noprint;
            select count(*) into :_chall_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&mem.")
              and upcase(name)=upcase("&col.");
        quit;

        %if &_chall_has_col.=0 %then %do;
            %if &req.=1 %then %do;
                %put ERROR: [challenge_contract] &col. no encontrada en &mem..;
                %let _chall_rc=1;
            %end;
            %else %do;
                %put WARNING: [challenge_contract] &col. no encontrada en
                    &mem..;
            %end;
        %end;
    %mend _chall_chk;

    %_chall_chk(mem=&train_table., col=&target., req=1);
    %if &_chall_rc.=1 %then %return;
    %_chall_chk(mem=&oot_table., col=&target., req=1);
    %if &_chall_rc.=1 %then %return;

    %_chall_chk(mem=&train_table., col=&score_var., req=1);
    %if &_chall_rc.=1 %then %return;
    %_chall_chk(mem=&oot_table., col=&score_var., req=1);
    %if &_chall_rc.=1 %then %return;

    %_chall_chk(mem=&train_table., col=&byvar., req=1);
    %if &_chall_rc.=1 %then %return;
    %_chall_chk(mem=&oot_table., col=&byvar., req=1);
    %if &_chall_rc.=1 %then %return;

    %if %length(%superq(id_var)) > 0 %then %do;
        %_chall_chk(mem=&train_table., col=&id_var., req=0);
        %_chall_chk(mem=&oot_table., col=&id_var., req=0);
    %end;

    %if %length(%superq(var_seg)) > 0 %then %do;
        %_chall_chk(mem=&train_table., col=&var_seg., req=0);
        %_chall_chk(mem=&oot_table., col=&var_seg., req=0);
    %end;

    %put NOTE: [challenge_contract] OK - TRAIN=&_chall_nobs_trn. OOT=&_chall_nobs_oot.;
    %put NOTE: [challenge_contract] target=&target. score=&score_var.
        byvar=&byvar. id_var=&id_var. var_seg=&var_seg..;
    %put NOTE: [challenge_contract] vars_num=&vars_num.;
    %put NOTE: [challenge_contract] vars_cat=&vars_cat.;
%mend challenge_contract;
