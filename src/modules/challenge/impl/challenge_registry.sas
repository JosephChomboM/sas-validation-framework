/* =========================================================================
challenge_registry.sas - Registry generico para METOD9 Challenge
========================================================================= */

%macro _chall_build_registry(results=work._chall_topn_results, troncal_id=,
    scope=, seg_id=, segment_label=, var_seg=, astore_name=, models_path=,
    algo_name=Gradient Boosting, algo_code=gb, artifact_prefix=,
    out=work._chall_registry);

    proc sort data=&results. out=work._chall_registry_sorted;
        by descending Gini_Penalizado;
    run;

    data &out.;
        length Algo_Name $32 Algo_Code $16 Scope $16 Segment_Label $32
            Var_Seg $64 Astore_Name $128 Models_Path $512
            Artifact_Prefix $128;
        set work._chall_registry_sorted;
        troncal_id=&troncal_id.;
        Scope="&scope.";
        seg_id=&seg_id.;
        Segment_Label="&segment_label.";
        Var_Seg="&var_seg.";
        Algo_Name="&algo_name.";
        Algo_Code="&algo_code.";
        Model_Rank=_n_;
        Astore_Name="&astore_name.";
        Models_Path="&models_path.";
        Artifact_Prefix="&artifact_prefix.";
        Is_Champion=(Model_Rank=1);
    run;

    proc datasets library=work nolist nowarn;
        delete _chall_registry_sorted;
    quit;
%mend _chall_build_registry;
