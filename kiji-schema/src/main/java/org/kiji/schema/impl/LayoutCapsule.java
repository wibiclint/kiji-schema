package org.kiji.schema.impl;

import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.ColumnNameTranslator;

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
  public ColumnNameTranslator getColumnNameTranslator();

  /**
   * Get the KijiTable to which this capsule is associated.
   * @return the KijiTable to which this capsule is associated.
   */
  public KijiTable getTable();
}
