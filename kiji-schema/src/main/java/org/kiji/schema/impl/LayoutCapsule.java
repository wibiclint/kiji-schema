package org.kiji.schema.impl;

import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;

public interface LayoutCapsule {
  /**
   * Get the KijiTableLayout for the associated layout.
   * @return the KijiTableLayout for the associated layout.
   */
  public KijiTableLayout getLayout();

  /**
   * Get the ColumnNameTranslator for the associated layout.
   * @return the ColumnNameTranslator for the associated layout.
   */
  public KijiColumnNameTranslator getKijiColumnNameTranslator();

}
