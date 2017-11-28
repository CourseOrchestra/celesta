package ru.curs.lyra.grid;

import java.awt.GridLayout;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JSlider;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import ru.curs.celesta.CelestaException;

public class Main extends JFrame {

	private static final long serialVersionUID = 1L;
	private static final int width = 800;
	private static final int height = 200;
	public static final Main MAIN = new Main();
	private JSlider sl = new JSlider();
	private JLabel lab = new JLabel();

	public static final int MAX = 1000;

	private VarcharFieldEnumerator mgr1;

	{
		try {
			mgr1 = new VarcharFieldEnumerator(VarcharFieldEnumeratorTest.DBA, "ваня", "маша", 4);
		} catch (CelestaException e) {
			e.printStackTrace();
		}

	}

	private IntFieldEnumerator mgr2 = new IntFieldEnumerator(0, 1000);
	private CompositeKeyEnumerator mgr = new CompositeKeyEnumerator(mgr1, mgr2);

	public Main() {

		setSize(width, height * 2);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		// setResizable(false);
		setLocationRelativeTo(null);
		setVisible(true);
		this.add(lab);
		this.getContentPane().add(lab);
		this.getContentPane().add(sl);
		sl.setMaximum(MAX);

		this.setLayout(new GridLayout(2, 1));
		sl.addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent ev) {
				double q = sl.getValue() / (double) MAX;
				mgr.setPosition(q);
				lab.setText(String.format("%s-%d", mgr1.getValue(), mgr2.getValue()));
			}
		});
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}
}
