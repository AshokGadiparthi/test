function getChildren(
  list: Master[],
  cust_id: string,
  acct_num: string,
  custlineseq: number
): Child[] {
  const master = list.find(
    m =>
      m.cust_id === cust_id &&
      m.acct_num === acct_num &&
      m.custlineseq === custlineseq
  );
  return master ? master.children : [];
}

// Example usage
const result = getChildren(masterList, "C001", "A100", 1);
console.log(result);
// → [ { childId: 101, value: 'x' }, { childId: 102, value: 'y' } ]
