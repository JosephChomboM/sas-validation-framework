/* =========================================================================
precision_contract.sas - Validaciones pre-ejecucion del modulo Precision
========================================================================= */
%macro precision_contract(input_caslib=, train_table=, oot_table=, target=,
    score_var=, byvar=, def_cld=, monto_var=, segvar=);

    %let _prec_rc=0;

    %local _prec_nobs_trn _prec_nobs_oot _prec_has_col;

    %if %length(%superq(target))=0 %then %do;
        %put ERROR: [precision_contract] target no definido.;
        %let _prec_rc=1;
        %return;
    %end;

    %if %length(%superq(score_var))=0 %then %do;
        %put ERROR: [precision_contract] score_var no definido.;
        %let _prec_rc=1;
        %return;
    %end;

    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [precision_contract] byvar no definido.;
        %let _prec_rc=1;
        %return;
    %end;

    %if %length(%superq(def_cld))=0 %then %do;
        %put ERROR: [precision_contract] def_cld no definido.;
        %let _prec_rc=1;
        %return;
    %end;

    proc sql noprint;
        select count(*) into :_prec_nobs_trn trimmed
        from &input_caslib..&train_table.;
        select count(*) into :_prec_nobs_oot trimmed
        from &input_caslib..&oot_table.;
    quit;

    %if &_prec_nobs_trn.=0 %then %do;
        %put ERROR: [precision_contract] TRAIN vacio o no accesible.;
        %let _prec_rc=1;
        %return;
    %end;

    %if &_prec_nobs_oot.=0 %then %do;
        %put ERROR: [precision_contract] OOT vacio o no accesible.;
        %let _prec_rc=1;
        %return;
    %end;

    %macro _prec_chk(mem=, col=, req=1);
        %let _prec_has_col=0;
        proc sql noprint;
            select count(*) into :_prec_has_col trimmed
            from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.")
              and upcase(memname)=upcase("&mem.")
              and upcase(name)=upcase("&col.");
        quit;
        %if &_prec_has_col.=0 %then %do;
            %if &req.=1 %then %do;
                %put ERROR: [precision_contract] &col. no encontrada en &mem..;
                %let _prec_rc=1;
            %end;
            %else %do;
                %put WARNING: [precision_contract] &col. no encontrada en &mem..;
            %end;
        %end;
    %mend;

    %_prec_chk(mem=&train_table., col=&target., req=1);
    %if &_prec_rc.=1 %then %return;
    %_prec_chk(mem=&oot_table., col=&target., req=1);
    %if &_prec_rc.=1 %then %return;

    %_prec_chk(mem=&train_table., col=&score_var., req=1);
    %if &_prec_rc.=1 %then %return;
    %_prec_chk(mem=&oot_table., col=&score_var., req=1);
    %if &_prec_rc.=1 %then %return;

    %_prec_chk(mem=&train_table., col=&byvar., req=1);
    %if &_prec_rc.=1 %then %return;
    %_prec_chk(mem=&oot_table., col=&byvar., req=1);
    %if &_prec_rc.=1 %then %return;

    %if %length(%superq(monto_var)) > 0 %then %do;
        %_prec_chk(mem=&train_table., col=&monto_var., req=0);
        %_prec_chk(mem=&oot_table., col=&monto_var., req=0);
    %end;

    %if %length(%superq(segvar)) > 0 %then %do;
        %_prec_chk(mem=&train_table., col=&segvar., req=0);
        %_prec_chk(mem=&oot_table., col=&segvar., req=0);
    %end;

    %put NOTE: [precision_contract] OK - target=&target. score=&score_var.
        byvar=&byvar. def_cld=&def_cld. monto=&monto_var. segvar=&segvar.;

%mend precision_contract;
