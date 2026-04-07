use crate::metric::Metric;

/// Returns a value where lower = more similar (for use in a min-heap).
pub fn distance(metric: Metric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        Metric::Cosine => cosine_distance(a, b),
        Metric::Dot => -dot_product(a, b), // negate so lower = better
        Metric::L2 => l2_distance(a, b),
    }
}

pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// 1 - cosine_similarity
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product(a, b);
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    1.0 - dot / (norm_a * norm_b + f32::EPSILON)
}

/// Euclidean distance
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dot_product_known_values() {
        let a = [1.0_f32, 2.0, 3.0];
        let b = [4.0_f32, 5.0, 6.0];
        let result = dot_product(&a, &b);
        assert!((result - 32.0).abs() < 1e-5, "expected 32.0, got {result}");
    }

    #[test]
    fn dot_product_zero_vectors() {
        let a = [0.0_f32, 0.0, 0.0];
        let b = [1.0_f32, 2.0, 3.0];
        assert_eq!(dot_product(&a, &b), 0.0);
    }

    #[test]
    fn dot_product_identical_vectors() {
        let a = [1.0_f32, 0.0, 0.0];
        assert!((dot_product(&a, &a) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn dot_product_empty_slice() {
        assert_eq!(dot_product(&[], &[]), 0.0);
    }

    #[test]
    fn cosine_distance_identical_vectors() {
        let a = [1.0_f32, 2.0, 3.0];
        // cosine distance between identical vectors should be ~0
        let d = cosine_distance(&a, &a);
        assert!(d.abs() < 1e-5, "expected ~0.0, got {d}");
    }

    #[test]
    fn cosine_distance_orthogonal_vectors() {
        let a = [1.0_f32, 0.0];
        let b = [0.0_f32, 1.0];
        let d = cosine_distance(&a, &b);
        // cosine_similarity = 0, so distance = 1
        assert!((d - 1.0).abs() < 1e-5, "expected 1.0, got {d}");
    }

    #[test]
    fn cosine_distance_zero_vector() {
        let a = [0.0_f32, 0.0, 0.0];
        let b = [1.0_f32, 2.0, 3.0];
        // norm_a = 0, denominator = EPSILON, result should be finite
        let d = cosine_distance(&a, &b);
        assert!(d.is_finite());
    }

    #[test]
    fn cosine_distance_empty_slice() {
        let d = cosine_distance(&[], &[]);
        assert!(d.is_finite());
    }

    #[test]
    fn l2_distance_identical_vectors() {
        let a = [1.0_f32, 2.0, 3.0];
        let d = l2_distance(&a, &a);
        assert!(d.abs() < 1e-5, "expected 0.0, got {d}");
    }

    #[test]
    fn l2_distance_known_values() {
        let a = [0.0_f32, 0.0];
        let b = [3.0_f32, 4.0];
        let d = l2_distance(&a, &b);
        assert!((d - 5.0).abs() < 1e-5, "expected 5.0, got {d}");
    }

    #[test]
    fn l2_distance_zero_vector() {
        let a = [0.0_f32, 0.0, 0.0];
        let b = [0.0_f32, 0.0, 0.0];
        assert_eq!(l2_distance(&a, &b), 0.0);
    }

    #[test]
    fn l2_distance_empty_slice() {
        assert_eq!(l2_distance(&[], &[]), 0.0);
    }

    #[test]
    fn distance_dot_negated() {
        let a = [1.0_f32, 2.0, 3.0];
        let b = [4.0_f32, 5.0, 6.0];
        let d = distance(Metric::Dot, &a, &b);
        assert!((d - (-32.0)).abs() < 1e-5, "expected -32.0, got {d}");
    }

    #[test]
    fn distance_l2_consistent() {
        let a = [0.0_f32, 0.0];
        let b = [3.0_f32, 4.0];
        assert!((distance(Metric::L2, &a, &b) - 5.0).abs() < 1e-5);
    }
}
