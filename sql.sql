--화면쿼리 
/*SQL_ID: kr.ap.dbt.gapmgnt.prfranl.ec.qc.impl.ClgtAnlQcImpl.getBrspnEmpClgtAnlInq*/
WITH prdsupp as(
		SELECT max(stnd_yrmn) AS stnd_yrmn	 
			  ,max(dbty_srts_id) AS dbty_srts_id
			  ,max(prd_supp_prtn_id) AS prd_supp_prtn_id
			  ,max(stnd_mth_sal_qty) AS stnd_mth_sal_qty
			  ,max(stnd_mth_slrt_qty) AS stnd_mth_slrt_qty
			  ,max(stnd_mth_sal_amt) AS stnd_mth_sal_amt
			  ,max(stnd_mth_slrt_amt) AS stnd_mth_slrt_amt
			  ,max(avg_prch_accm_amt) AS avg_prch_accm_amt
			  ,max(tt_rtst_prc_amt) AS tt_rtst_prc_amt
			  ,max(stnd_mth_ost_adjd_sum_amt) AS stnd_mth_ost_adjd_sum_amt
			  ,max(tt_cnt_amt) AS tt_cnt_amt
		FROM (
				SELECT A.stnd_yrmn AS stnd_yrmn					--기준년월
			  		,A.dbty_srts_id AS dbty_srts_id			--소매점ID
			  		,A.prd_supp_prtn_id AS prd_supp_prtn_id   --상품공급거래처ID
			  		,B.prd_cd	AS prd_cd						--상품코드
			  		,A.stnd_mth_sal_qty AS stnd_mth_sal_qty   --기준월판매수량
			  		,A.stnd_mth_slrt_qty AS stnd_mth_slrt_qty	--기준월환입수량
			  		,A.stnd_mth_sal_amt AS stnd_mth_sal_amt   --기준월판매금액
			  		,A.stnd_mth_slrt_amt AS stnd_mth_slrt_amt	--기준월환입금액
			  		,A.avg_prch_accm_amt AS avg_prch_accm_amt	--평균매입누계금액
			  		,A.tt_rtst_prc_amt AS tt_rtst_prc_amt		--총대리점가격금액
			  		,A.stnd_mth_ost_adjd_sum_amt AS stnd_mth_ost_adjd_sum_amt	--기준월미수조정합계금액
			  		,A.tt_cnt_amt AS tt_cnt_amt				--총수금액
				FROM dbt.psslsa_srtssupr_prfr A
					INNER JOIN (
								SELECT A.dbty_srts_id AS dbty_srts_id
									  ,A.prd_cd AS prd_cd
									  ,C.prd_supp_prtn_id AS prd_supp_prtn_id
								FROM 			dbt.psslsa_srtsprd_prfr A --소매점상품별 실적 테이블
								 INNER JOIN 	dbt.pspasm_srts B ON A.dbty_srts_id = B.dbty_srts_id
			 					LEFT OUTER JOIN scp.sccdem_product C ON A.prd_cd = C.prd_cd
			 					WHERE B.blng_prtn_id =  '11003359' 
			 					  AND A.stnd_yrmn >=  '202011' 
			 					  AND A.stnd_yrmn <=  '202011' 
			 					) B ON (A.prd_supp_prtn_id = B.prd_supp_prtn_id AND A.dbty_srts_id = B.dbty_srts_id)
								WHERE 1=1
								  AND A.stnd_yrmn >=  '202011' 
			 					  AND A.stnd_yrmn <=  '202011' 
		 										) AS A
		GROUP BY A.stnd_yrmn, A.dbty_srts_id, A.prd_supp_prtn_id
)
SELECT 
	  --A.dbty_srts_id AS dbty_srts_id 								--거래처코드
	   C.pemp_nm AS pemp_nm
	  --,max(B.srts_nm) AS srts_nm 									--거래처명
	  --,max(C.pemp_nm) AS pemp_nm 									--담당
	  ,sum(A.stnd_mth_sal_amt - A.stnd_mth_slrt_amt) AS stnd_mth_sal_amt		--매출액
	  ,sum(A.stnd_mth_ost_adjd_sum_amt) AS ost_adjd_sum_amt 		--에누리
	  ,sum(A.avg_prch_accm_amt) AS avg_prch_accm_amt 				--매출원가
	  ,round(CASE WHEN sum(tt_rtst_prc_amt) = 0 THEN 0
	  	   		  WHEN sum(tt_rtst_prc_amt) <> 0 AND sum(A.stnd_mth_sal_qty) - sum(A.stnd_mth_slrt_qty) < 0 THEN (1-(sum(avg_prch_accm_amt)/sum(tt_rtst_prc_amt)))*-100
	  	   		  WHEN sum(tt_rtst_prc_amt) <> 0 AND sum(A.stnd_mth_sal_qty) - sum(A.stnd_mth_slrt_qty) > 0 THEN (1-(sum(avg_prch_accm_amt)/sum(tt_rtst_prc_amt)))*100
		     END,1) AS dc_rt --할인율
	  ,sum((A.stnd_mth_sal_amt - A.stnd_mth_slrt_amt)-A.avg_prch_accm_amt) AS prft_amt--이익
	  ,sum(A.tt_cnt_amt) AS tt_cnt_amt								--수금
	  ,(
			SELECT 
		      	  sum(COALESCE(srts_eomt_ost_amt,0)) AS srts_eomt_ost_amt
			FROM(
	  			 SELECT DISTINCT a.prtn_id 
	  		   					,a.dbty_srts_id
	  		   					,LAST_VALUE(A.srts_eomt_ost_amt) OVER(PARTITION BY A.dbty_srts_id ORDER BY A.ldgr_stnd_yrmn RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS srts_eomt_ost_amt
	  			 FROM dbt.psslsa_srts_mn_lgr a
	  	 		   INNER JOIN dbt.pspasm_srts t ON (t.dbty_srts_id = a.dbty_srts_id)
	  			 WHERE t.rspn_prtn_emp_id = c.prtn_emp_id
	  			   AND a.prtn_id =  '11003359' 
	  			) a 
	  		GROUP BY a.prtn_id
	   ) AS eomt_ost_amt											--현미수
FROM prdsupp A
INNER JOIN dbt.pspasm_srts B ON A.dbty_srts_id = B.dbty_srts_id
LEFT OUTER JOIN psp.pspacm_pemp C ON B.rspn_prtn_emp_id = C.prtn_emp_id
WHERE 1=1
	AND B.blng_prtn_id =  '11003359' --대리점
				AND A.stnd_yrmn >=  '202011' --기간시작		
	AND A.stnd_yrmn <=  '202011' --기간끝	
GROUP BY C.pemp_nm, C.prtn_emp_id
ORDER BY 1


--오즈 쿼리 
WITH prdsupp as(																													
					SELECT max(stnd_yrmn) AS stnd_yrmn																					
		  				  ,max(dbty_srts_id) AS dbty_srts_id																			
		  				  ,max(prd_supp_prtn_id) AS prd_supp_prtn_id																	
		  				  ,max(stnd_mth_sal_qty) AS stnd_mth_sal_qty																	
						  ,max(stnd_mth_slrt_qty) AS stnd_mth_slrt_qty																	
						  ,max(stnd_mth_sal_amt) AS stnd_mth_sal_amt																	
						  ,max(stnd_mth_slrt_amt) AS stnd_mth_slrt_amt																	
						  ,max(avg_prch_accm_amt) AS avg_prch_accm_amt																	
						  ,max(tt_rtst_prc_amt) AS tt_rtst_prc_amt																		
						  ,max(stnd_mth_ost_adjd_sum_amt) AS stnd_mth_ost_adjd_sum_amt													
						  ,max(tt_cnt_amt) AS tt_cnt_amt																				
					FROM (																												
							SELECT A.stnd_yrmn AS stnd_yrmn					--기준년월														
			  					  ,A.dbty_srts_id AS dbty_srts_id			--소매점ID														
			  					  ,A.prd_supp_prtn_id AS prd_supp_prtn_id   --상품공급거래처ID												
			  					  ,B.prd_cd	AS prd_cd						--상품코드														
			  					  ,A.stnd_mth_sal_qty AS stnd_mth_sal_qty   --기준월판매수량													
			  					  ,A.stnd_mth_slrt_qty AS stnd_mth_slrt_qty	--기준월환입수량													
			  					  ,A.stnd_mth_sal_amt AS stnd_mth_sal_amt   --기준월판매금액													
			  					  ,A.stnd_mth_slrt_amt AS stnd_mth_slrt_amt	--기준월환입금액													
			  					  ,A.avg_prch_accm_amt AS avg_prch_accm_amt	--평균매입누계금액													
			  					  ,A.tt_rtst_prc_amt AS tt_rtst_prc_amt		--총대리점가격금액													
			  					  ,A.stnd_mth_ost_adjd_sum_amt AS stnd_mth_ost_adjd_sum_amt	--기준월미수조정합계금액								
			  					  ,A.tt_cnt_amt AS tt_cnt_amt				--총수금액														
							FROM dbt.psslsa_srtssupr_prfr A																				
							  INNER JOIN (																								
											SELECT A.dbty_srts_id AS dbty_srts_id														
									  			  ,A.prd_cd AS prd_cd																	
									  			  ,C.prd_supp_prtn_id AS prd_supp_prtn_id												
											FROM dbt.psslsa_srtsprd_prfr A --소매점상품별 실적 테이블												
								 			  INNER JOIN dbt.pspasm_srts B ON A.dbty_srts_id = B.dbty_srts_id							
			 								  LEFT OUTER JOIN scp.sccdem_product C ON A.prd_cd = C.prd_cd								
			 								WHERE B.blng_prtn_id = '11003359'												
											  AND A.stnd_yrmn >= '202011'												
											  AND A.stnd_yrmn <= '202011'												
			 							  ) B ON (A.prd_supp_prtn_id = B.prd_supp_prtn_id AND A.dbty_srts_id = B.dbty_srts_id)			
							WHERE 1=1																									
						) AS A																											
					GROUP BY A.stnd_yrmn, A.dbty_srts_id, A.prd_supp_prtn_id															
	)																																	
	SELECT 																																
	   	--A.dbty_srts_id AS dbty_srts_id 								--거래처코드															
	   	  C.pemp_nm AS pemp_nm																											
	    --,max(B.srts_nm) AS srts_nm 									--거래처명															
	  	--,max(C.pemp_nm) AS pemp_nm 									--담당															
	  	  ,sum(A.stnd_mth_sal_amt - A.stnd_mth_slrt_amt) AS stnd_mth_sal_amt		--매출액												
	  	  ,sum(A.stnd_mth_ost_adjd_sum_amt) AS ost_adjd_sum_amt 		--에누리															
	  	  ,sum(A.avg_prch_accm_amt) AS avg_prch_accm_amt 				--매출원가															
	  	  ,round(CASE WHEN sum(tt_rtst_prc_amt) = 0 THEN 0																				
	  	   		  	  ELSE (1-(sum(avg_prch_accm_amt)/sum(tt_rtst_prc_amt)))*100														
		     	 END,1) AS dc_rt --할인율																									
	  	  ,sum((A.stnd_mth_sal_amt - A.stnd_mth_slrt_amt)-A.avg_prch_accm_amt) AS prft_amt--이익											
	  	  ,sum(A.tt_cnt_amt) AS tt_cnt_amt								--수금															
	  	  ,sum(D.srts_eomt_ost_amt) AS eomt_ost_amt				--현미수																	
	  	--,sum(COALESCE(D.srts_eomt_ost_amt,0)) AS eomt_ost_amt 		--현미수															
	FROM prdsupp A																														
	  INNER JOIN dbt.pspasm_srts B ON A.dbty_srts_id = B.dbty_srts_id																	
	  LEFT OUTER JOIN psp.pspacm_pemp C ON B.rspn_prtn_emp_id = C.prtn_emp_id															
	  LEFT OUTER JOIN (																													
		  				SELECT  prtn_id 																								
		  				   	   ,dbty_srts_id																							
		  				   	   ,max(COALESCE(srts_eomt_ost_amt,0)) AS srts_eomt_ost_amt													
	  		  			FROM(																											
	  		  			  	   SELECT prtn_id, dbty_srts_id, FIRST_VALUE(srts_eomt_ost_amt) OVER(PARTITION BY dbty_srts_id ORDER BY ldgr_stnd_yrmn) AS srts_eomt_ost_amt		
	  		  			  	   FROM dbt.psslsa_srts_mn_lgr																				
	  		  			  	   WHERE prtn_id = '11003359'																	
	  			  			 ) AS W																										
	  					GROUP BY prtn_id,dbty_srts_id																					
					   ) D ON (A.dbty_srts_id = D.dbty_srts_id AND  B.blng_prtn_id = D.prtn_id)										 	
	WHERE 1=1																															
	  AND B.blng_prtn_id = '11003359' --대리점																					
		AND A.stnd_yrmn >= '202011'  --기간시작																		
		AND A.stnd_yrmn <= '202011' --기간끝																			
	GROUP BY C.pemp_nm																													
	ORDER BY 1
	