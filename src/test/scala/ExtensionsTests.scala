import org.scalatest.{Matchers, WordSpec}
import com.holdenkarau.spark.testing
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion

class ExtensionsTests extends WordSpec with Matchers with SharedSparkContext {

  val text =
    """
      |The vast majority of animal species undergo the process of aging. Whilst aging is a nearly universal occurrence, it should be noted that other medical problems such as muscle wastage leading to sarcopenia, reduction in bone mass and density leading to osteoporosis, increased arterial hardening resulting in hypertension, atherosclerosis, and brain tissue atrophy resulting in dementia, all of which are nearly universal in humans, are classified as diseases in need of medical interventions (
    """.stripMargin

//  "spark test" in {
//    val words: List[String] = text.split(" ").toList
//    val rdd: RDD[String] = sc.parallelize(words)
//    }
}