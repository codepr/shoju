use shoju::partition::Partition;

mod smoke_test {
    use shoju::partition::Partition;
    pub fn generate_partition(partition: &mut Partition, n: i32) -> std::io::Result<()> {
        for _i in 0..n {
            partition
                .append_record(Some("key".into()), &[0, 0, 1, 0])
                .expect("Error writing to disk");
        }
        partition.flush()
    }

    pub fn replay_log(partition: &mut Partition, offsets: &[u64]) {
        for offset in offsets.iter() {
            let r = partition
                .find_record(*offset)
                .expect(&format!("Failed lookup {}", offset));
            println!("{}", r);
        }
    }
}

fn main() -> std::io::Result<()> {
    let mut partition = Partition::init()?;
    // smoke_test::generate_partition(&mut partition, 1200)?;
    smoke_test::replay_log(
        &mut partition,
        &[
            0, 9, 10, 14, 53, 163, 208, 400, 499, 563, 957, 980, 1010, 1400,
        ],
    );
    Ok(())
}
