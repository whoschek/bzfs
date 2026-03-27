---
name: bzfs-improve-code-coverage
description:
  Improve code coverage in this repository with project-approved coverage commands and meaningful test additions. Use
  when asked to increase coverage, identify uncovered branches/functions, or report before-vs-after coverage
  percentages.
---

# bzfs Improve Code Coverage

## Outcome

Improve code coverage meaningfully by adding high-value tests for critical uncovered logic paths and reporting
measurable results.

## Workflow

1. Measure baseline coverage:

   ```bash
   export bzfs_test_mode="${bzfs_test_mode-unit}"  # use existing bzfs_test_mode if defined
   python3 -m coverage run -m bzfs_tests.test_all  # run the tests to gather coverage data
   python3 -m coverage xml                         # generate an XML coverage report
   cat coverage.xml                                # view the XML report to identify uncovered lines/branches
   ```

2. Identify critical gaps:
   - Use `coverage.xml` to identify key logic branches or functions that lack tests.

3. Add high-value tests:
   - Prioritize adding tests for those areas rather than chasing minor unused lines.
   - **Focus on adding meaningful high-value tests:** Do not add low-value tests just to increase a coverage percentage.

4. Re-run measurement with the same commands and compare results.

5. Report results:
   - State the **before vs. after** coverage percentage so the impact is clear.
   - Summarize which critical branches/functions are now covered.
