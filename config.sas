/* ======================= CONFIG.SAS (SOURCE OF TRUTH) =======================
   Generado desde HTML.  Declara configuración y construye tablas CAS
   (casuser) para iteración del framework.
   NO contiene lógica de orquestación; el framework la ejecuta.

   Esquema estricto — campos exactos requeridos:
     cfg_troncales : 1 fila por troncal
     cfg_segmentos : 1 fila por (troncal_id, seg_id), overrides opcionales

   Los parámetros de usuario (fw_root, ADLS, methods) se configuran en
   los archivos steps/*.sas (frontend).  Este archivo solo genera las
   tablas CAS de configuración de troncales y segmentos.

   Requisitos: sesión CAS activa y caslib CASUSER accesible.
   =========================================================================== */

/* ---------- 1) casuser.cfg_troncales (1 fila por troncal) ---------- */
data casuser.cfg_troncales;
	length
	troncal_id      8
	id_var_id       $64
	target          $64
	pd              $64
	xb              $64
	monto           $64
	byvar           $64
	train_min_mes   8
	train_max_mes   8
	oot_min_mes     8
	oot_max_mes     8
	def_cld         8
	num_unv         $4000
	cat_unv         $4000
	dri_num_unv     $4000
	dri_cat_unv     $4000
	var_seg         $64
	n_segments      8
	;

	/* ---------------- Troncal 1 ---------------- */
	troncal_id      = 1;
	id_var_id       = "CODCLAVECIC";
	target          = "DEF12";
	pd              = "PD_MOD_CLI_DEP_2Q24";
	xb              = "PD_MOD_CLI_DEP_2Q24";
	monto           = "Monto_dummy";
	byvar           = "CODMES";

	train_min_mes   = 202301;
	train_max_mes   = 202310;
	oot_min_mes     = 202311;
	oot_max_mes     = 202401;
	def_cld         = 202401;

	num_unv         = "prod_antmax_per_prm_u12 FATC_MTO_INT_TC_ING_U3M can_ctd_tmo_CF_prm_u12 INGRESO_BCI prod_antmax_cvi_prm_u12";
	cat_unv         = "";
	dri_num_unv     = "";
	dri_cat_unv     = "";

	var_seg         = "SEGMENTO_NUM";
	n_segments      = 3;

	output;

	/* ---------------- Troncal 2  ---------------- */
	troncal_id      = 2;
	id_var_id       = "CODCLAVECIC";
	target          = "DEF12";
	pd              = "PD_MOD_CLI_DEP_2Q24";
	xb              = "PD_MOD_CLI_DEP_2Q24";
	monto           = "Monto_dummy";
	byvar           = "CODMES";

	train_min_mes   = 202301;
	train_max_mes   = 202308;
	oot_min_mes     = 202309;
	oot_max_mes     = 202401;
	def_cld         = 202401;

	num_unv         = "prod_antmax_per_prm_u12 FATC_MTO_INT_TC_ING_U3M can_ctd_tmo_CF_prm_u12 prod_mto_sld_prm_pas_max_6 prod_mto_sld_fim_pas_max_8 prod_antmin_tot_prm_u9m fatc_mto_int_ccl_frq_u3m pllchf_ctd_pas_pr12r";
	cat_unv         = "";
	dri_num_unv     = "";
	dri_cat_unv     = "";

	var_seg         = "SEGMENTO_NUM";
	n_segments      = 3;

	output;
run;

/* ---------- 2) casuser.cfg_segmentos (overrides por segmento) ---------- */
/*  Clave: (troncal_id, seg_id).  Ambos numéricos.                        */
/*  Si las listas quedan en blanco, el segmento hereda del troncal.        */
data casuser.cfg_segmentos;
	length
	troncal_id    8
	seg_id        8
	num_list      $4000
	cat_list      $4000
	dri_num_list  $4000
	dri_cat_list  $4000
	;

	/* Troncal 1, seg 1 */
	troncal_id   = 1;  seg_id = 1;
	num_list     = "prod_antmax_per_prm_u12 FATC_MTO_INT_TC_ING_U3M can_ctd_tmo_CF_prm_u12";   /* vacío => hereda num_unv del troncal */
	cat_list     = "";
	dri_num_list = "can_ctd_tmo_CF_prm_u12";
	dri_cat_list = "";
	output;

	/* Troncal 1, seg 2 */
	troncal_id   = 1;  seg_id = 2;
	num_list     = "fatc_pct_pag_med_ctamin_u3 can_ctd_tmo_CF_prm_u12 prod_mto_sld_prm_tsav_frq7 INGRESO_BCI";
	cat_list     = "";
	dri_num_list = "fatc_pct_pag_med_ctamin_u3";
	dri_cat_list = "";
	output;

	/* Troncal 1, seg 3 */
	troncal_id   = 1;  seg_id = 3;
	num_list     = "fatc_pct_pag_med_ctamin_u3 can_ctd_tmo_CF_prm_u12 prod_mto_sld_prm_tsav_frq7 INGRESO_BCI prod_antmax_cvi_prm_u12";
	cat_list     = "";
	dri_num_list = "INGRESO_BCI";
	dri_cat_list = "";
	output;

	/* Troncal 2, seg 1 */
	troncal_id   = 2;  seg_id = 1;
	num_list     = "prod_antmax_per_prm_u12 FATC_MTO_INT_TC_ING_U3M can_ctd_tmo_CF_prm_u12";
	cat_list     = "";
	dri_num_list = "FATC_MTO_INT_TC_ING_U3M";
	dri_cat_list = "";
	output;

	/* Troncal 2, seg 2 */
	troncal_id   = 2;  seg_id = 2;
	num_list     = "prod_mto_sld_prm_pas_max_6 prod_mto_sld_fim_pas_max_8 prod_antmin_tot_prm_u9m fatc_mto_int_ccl_frq_u3m pllchf_ctd_pas_pr12r";
	cat_list     = "";
	dri_num_list = "";
	dri_cat_list = "";
	output;
run;
