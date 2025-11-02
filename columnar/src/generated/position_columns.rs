#[derive(Default, Debug)]
pub struct PositionVecColumns {
    pub rcid: crate::VecColumn<i32>,
    pub industry_id: crate::VecColumn<u16>,
    pub company_id: crate::VecColumn<u32>,
    pub rics_k400_index: crate::VecColumn<u16>,
    pub rics_k50_index: crate::VecColumn<u8>,
    pub rics_k10_index: crate::VecColumn<u8>,
    pub prev_rcid: crate::VecColumn<i32>,
    pub next_rcid: crate::VecColumn<i32>,
    pub start_index: crate::VecColumn<i16>,
    pub end_index: crate::VecColumn<i16>,
    pub next_start_index: crate::VecColumn<i16>,
    pub prev_end_index: crate::VecColumn<i16>,
    pub weight: crate::VecColumn<f32>,
    pub sample_weight: crate::VecColumn<f32>,
    pub inflow_weight: crate::VecColumn<f32>,
    pub outflow_weight: crate::VecColumn<f32>,
    pub full_time_prob: crate::VecColumn<f32>,
    pub multiplicator: crate::VecColumn<f32>,
    pub inflation: crate::VecColumn<f32>,
    pub total_compensation_ratio: crate::VecColumn<f32>,
    pub full_time_hours: crate::VecColumn<f32>,
    pub estimated_us_log_salary: crate::VecColumn<f32>,
    pub f_prob: crate::VecColumn<f32>,
    pub white_prob: crate::VecColumn<f32>,
    pub multiple_prob: crate::VecColumn<f32>,
    pub black_prob: crate::VecColumn<f32>,
    pub api_prob: crate::VecColumn<f32>,
    pub hispanic_prob: crate::VecColumn<f32>,
    pub native_prob: crate::VecColumn<f32>,
    pub role_v3_index: crate::VecColumn<u16>,
    pub state: crate::VecColumn<i16>,
    pub msa: crate::VecColumn<i16>,
    pub mapped_role: crate::VecColumn<i16>,
    pub country: crate::VecColumn<i16>,
    pub region: crate::VecColumn<i16>,
    pub seniority: crate::VecColumn<i16>,
    pub highest_degree: crate::VecColumn<i16>,
    pub internal_outflow: crate::VecColumn<bool>,
    pub internal_inflow: crate::VecColumn<bool>,
    pub mapped_skills_v3: crate::VecColumn<Vec<u16>>,
    pub description: crate::VecColumn<String>,
    pub raw_title: crate::VecColumn<String>,
}
impl crate::ColumnBundle<crate::models::position::Position> for PositionVecColumns {
    fn push(&mut self, row: &crate::models::position::Position) {
        self.rcid.push(&row.rcid);
        self.industry_id.push(&row.industry_id);
        self.company_id.push(&row.company_id);
        self.rics_k400_index.push(&row.rics_k400_index);
        self.rics_k50_index.push(&row.rics_k50_index);
        self.rics_k10_index.push(&row.rics_k10_index);
        self.prev_rcid.push(&row.prev_rcid);
        self.next_rcid.push(&row.next_rcid);
        self.start_index.push(&row.start_index);
        self.end_index.push(&row.end_index);
        self.next_start_index.push(&row.next_start_index);
        self.prev_end_index.push(&row.prev_end_index);
        self.weight.push(&row.weight);
        self.sample_weight.push(&row.sample_weight);
        self.inflow_weight.push(&row.inflow_weight);
        self.outflow_weight.push(&row.outflow_weight);
        self.full_time_prob.push(&row.full_time_prob);
        self.multiplicator.push(&row.multiplicator);
        self.inflation.push(&row.inflation);
        self.total_compensation_ratio
            .push(&row.total_compensation_ratio);
        self.full_time_hours.push(&row.full_time_hours);
        self.estimated_us_log_salary
            .push(&row.estimated_us_log_salary);
        self.f_prob.push(&row.f_prob);
        self.white_prob.push(&row.white_prob);
        self.multiple_prob.push(&row.multiple_prob);
        self.black_prob.push(&row.black_prob);
        self.api_prob.push(&row.api_prob);
        self.hispanic_prob.push(&row.hispanic_prob);
        self.native_prob.push(&row.native_prob);
        self.role_v3_index.push(&row.role_v3_index);
        self.state.push(&row.state);
        self.msa.push(&row.msa);
        self.mapped_role.push(&row.mapped_role);
        self.country.push(&row.country);
        self.region.push(&row.region);
        self.seniority.push(&row.seniority);
        self.highest_degree.push(&row.highest_degree);
        self.internal_outflow.push(&row.internal_outflow);
        self.internal_inflow.push(&row.internal_inflow);
        self.mapped_skills_v3.push(&row.mapped_skills_v3);
        self.description.push(&row.description);
        self.raw_title.push(&row.raw_title);
    }
    fn merge(&mut self, other: Self) {
        self.rcid.merge(other.rcid);
        self.industry_id.merge(other.industry_id);
        self.company_id.merge(other.company_id);
        self.rics_k400_index.merge(other.rics_k400_index);
        self.rics_k50_index.merge(other.rics_k50_index);
        self.rics_k10_index.merge(other.rics_k10_index);
        self.prev_rcid.merge(other.prev_rcid);
        self.next_rcid.merge(other.next_rcid);
        self.start_index.merge(other.start_index);
        self.end_index.merge(other.end_index);
        self.next_start_index.merge(other.next_start_index);
        self.prev_end_index.merge(other.prev_end_index);
        self.weight.merge(other.weight);
        self.sample_weight.merge(other.sample_weight);
        self.inflow_weight.merge(other.inflow_weight);
        self.outflow_weight.merge(other.outflow_weight);
        self.full_time_prob.merge(other.full_time_prob);
        self.multiplicator.merge(other.multiplicator);
        self.inflation.merge(other.inflation);
        self.total_compensation_ratio
            .merge(other.total_compensation_ratio);
        self.full_time_hours.merge(other.full_time_hours);
        self.estimated_us_log_salary
            .merge(other.estimated_us_log_salary);
        self.f_prob.merge(other.f_prob);
        self.white_prob.merge(other.white_prob);
        self.multiple_prob.merge(other.multiple_prob);
        self.black_prob.merge(other.black_prob);
        self.api_prob.merge(other.api_prob);
        self.hispanic_prob.merge(other.hispanic_prob);
        self.native_prob.merge(other.native_prob);
        self.role_v3_index.merge(other.role_v3_index);
        self.state.merge(other.state);
        self.msa.merge(other.msa);
        self.mapped_role.merge(other.mapped_role);
        self.country.merge(other.country);
        self.region.merge(other.region);
        self.seniority.merge(other.seniority);
        self.highest_degree.merge(other.highest_degree);
        self.internal_outflow.merge(other.internal_outflow);
        self.internal_inflow.merge(other.internal_inflow);
        self.mapped_skills_v3.merge(other.mapped_skills_v3);
        self.description.merge(other.description);
        self.raw_title.merge(other.raw_title);
    }
}
impl crate::Columnar for crate::models::position::Position {
    type Columns = PositionVecColumns;
}
