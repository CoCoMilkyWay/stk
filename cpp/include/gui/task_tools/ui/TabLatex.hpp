#pragma once
#include <memory>
#include <string>

namespace tex {
class TeXRender;
}

namespace GUI::Tools {

struct LatexEditorState {

  char input_buffer[4096] = R"(
\[
  \begin{aligned}
    \mathcal{L}(\theta)
    &= \lim_{\varepsilon \to 0^+}
       \sup_{\substack{x \in \mathbb{R}^n \\ \|x\|_2 \le 1}}
       \Bigg\{
         \int_{\Omega}
         \frac{\partial}{\partial t}
         \left(
           \sum_{i=1}^{n} \alpha_i(t)\, x_i^{2}
         \right)
         \,\mathrm{d}\mu(\omega)
    \\
    &\quad
       - \frac{1}{2}
         \left\|
           \begin{pmatrix} A & B \\ C & D \end{pmatrix}
           \cdot
           \begin{pmatrix} x \\ y \end{pmatrix}
           -
           \mathbb{E}_{\mathbb{P}_\theta}
           \left[ f(X) \mid \mathcal{F}_t \right]
         \right\|_{\ell^p}^p
    \\
    &\quad
       + \sum_{k=0}^{\infty}
         \frac{(-1)^k}{(2k+1)!}
         \left(
           \oint_{\partial \Omega}
           \nabla \times \mathbf{F}
           \cdot \mathrm{d}\mathbf{r}
         \right)^{k}
    \\
    &\quad
       + \mathbf{1}_{\{x \in \mathbb{Q}^n\}}
         \;\oplus\;
         \forall \varepsilon > 0,\;
         \exists \delta > 0:
         \left(
           |x-y| < \delta \Rightarrow |f(x)-f(y)| < \varepsilon
         \right)
       \Bigg\}
    \\
    &\stackrel{\text{a.s.}}{=}
       \inf_{\theta \in \Theta}
       \left\{
         \mathrm{KL}\!\left( \mathbb{P}_\theta \,\|\, \mathbb{Q} \right)
         +
         \lambda
         \left( \det\left(\nabla^2 \phi(x)\right) \right)^{-\frac{1}{2}}
       \right\}
       \;\;\square
  \end{aligned}
\]
  )";
  std::unique_ptr<tex::TeXRender> render;
  float text_size = 20.0f;
  bool need_reparse = true;
  std::string error_msg;
  bool initialized = false;

  LatexEditorState();
  ~LatexEditorState();
};

void DrawLatexEditor(LatexEditorState &state);

} // namespace GUI::Tools
